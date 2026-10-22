/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Tests for Device::discard() (hole punching / TRIM), used by the block
// cache to give device space back when it is shrunk online. Kept apart from
// DeviceTest.cpp, which the OSS build does not compile.

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include <cstring>

#include "cachelib/common/Utils.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/testing/Callbacks.h"

using testing::_;

namespace facebook::cachelib::navy::tests {

TEST(DeviceDiscard, MemoryDevice) {
  constexpr uint32_t kSize = 16 * 1024;
  auto device = createMemoryDevice(kSize, nullptr /* encryptor */, 1024);
  Buffer wbuf = device->makeIOBuffer(kSize);
  std::memset(wbuf.data(), 0xab, kSize);
  EXPECT_TRUE(device->write(0, std::move(wbuf)));

  EXPECT_TRUE(device->discard(4 * 1024, 8 * 1024));
  EXPECT_EQ(8 * 1024, device->getBytesDiscarded());
  // Out of range is rejected
  EXPECT_FALSE(device->discard(kSize - 1024, 2 * 1024));
  EXPECT_EQ(8 * 1024, device->getBytesDiscarded());

  Buffer rbuf = device->makeIOBuffer(kSize);
  EXPECT_TRUE(device->read(0, kSize, rbuf.data()));
  for (uint32_t i = 0; i < kSize; i++) {
    const uint8_t expected = (i >= 4 * 1024 && i < 12 * 1024) ? 0 : 0xab;
    ASSERT_EQ(expected, rbuf.data()[i]) << i;
  }

  MockCounterVisitor visitor;
  EXPECT_CALL(visitor, call(_, _)).WillRepeatedly(testing::Return());
  EXPECT_CALL(visitor, call(strPiece("navy_device_bytes_discarded"), 8192));
  EXPECT_CALL(visitor, call(strPiece("navy_device_discard_errors"), 1));
  device->getCounters({toCallback(visitor)});
}

TEST(DeviceDiscard, FileDevice) {
  auto filePath = fmt::format("/tmp/DEVICE_DISCARD_TEST-{}", ::getpid());
  SCOPE_EXIT { util::removePath(filePath); };
  constexpr uint64_t kSize = 256 * 1024;
  constexpr uint32_t kAlign = 4096;
  auto device = createFileDevice(
      {filePath}, kSize, false /* truncateFile */, kAlign, 0 /* stripeSize */,
      0 /* maxDeviceWriteSize */, IoEngine::Sync, 0 /* qDepth */,
      false /* isFDPEnabled */, nullptr /* encryptor */,
      false /* isExclusiveOwner */);

  Buffer wbuf = device->makeIOBuffer(kSize);
  std::memset(wbuf.data(), 0xcd, kSize);
  EXPECT_TRUE(device->write(0, std::move(wbuf)));
  device->flush();

  struct stat before;
  ASSERT_EQ(0, ::stat(filePath.c_str(), &before));
  ASSERT_GT(before.st_blocks, 0);

  // Punch out the middle half
  EXPECT_TRUE(device->discard(kSize / 4, kSize / 2));
  EXPECT_EQ(kSize / 2, device->getBytesDiscarded());
  device->flush();

  struct stat after;
  ASSERT_EQ(0, ::stat(filePath.c_str(), &after));
  // Size is kept, blocks are given back to the file system
  EXPECT_EQ(before.st_size, after.st_size);
  EXPECT_LT(after.st_blocks, before.st_blocks);

  // The hole reads as zeros, the rest is intact
  Buffer rbuf = device->makeIOBuffer(kSize);
  EXPECT_TRUE(device->read(0, kSize, rbuf.data()));
  for (uint32_t i = 0; i < kSize; i++) {
    const uint8_t expected = (i >= kSize / 4 && i < 3 * kSize / 4) ? 0 : 0xcd;
    ASSERT_EQ(expected, rbuf.data()[i]) << i;
  }
}

TEST(DeviceDiscard, Raid0FileDevice) {
  auto filePath = fmt::format("/tmp/DEVICE_DISCARD_RAID_TEST-{}", ::getpid());
  std::vector<std::string> filePaths = {filePath + "_0", filePath + "_1"};
  SCOPE_EXIT {
    for (const auto& p : filePaths) {
      util::removePath(p);
    }
  };
  constexpr uint64_t kFileSize = 128 * 1024;
  constexpr uint32_t kAlign = 4096;
  constexpr uint32_t kStripe = 16 * 1024;
  auto device = createFileDevice(
      filePaths, kFileSize, false /* truncateFile */, kAlign, kStripe,
      0 /* maxDeviceWriteSize */, IoEngine::Sync, 0 /* qDepth */,
      false /* isFDPEnabled */, nullptr /* encryptor */,
      false /* isExclusiveOwner */);
  const uint64_t kSize = device->getSize();
  ASSERT_EQ(2 * kFileSize, kSize);

  Buffer wbuf = device->makeIOBuffer(kSize);
  std::memset(wbuf.data(), 0xef, kSize);
  EXPECT_TRUE(device->write(0, std::move(wbuf)));
  device->flush();

  struct stat before0, before1;
  ASSERT_EQ(0, ::stat(filePaths[0].c_str(), &before0));
  ASSERT_EQ(0, ::stat(filePaths[1].c_str(), &before1));

  // Discard the last 4 stripes of the device: 2 per file
  EXPECT_TRUE(device->discard(kSize - 4 * kStripe, 4 * kStripe));
  EXPECT_EQ(4 * kStripe, device->getBytesDiscarded());
  device->flush();

  struct stat after0, after1;
  ASSERT_EQ(0, ::stat(filePaths[0].c_str(), &after0));
  ASSERT_EQ(0, ::stat(filePaths[1].c_str(), &after1));
  EXPECT_EQ(before0.st_size, after0.st_size);
  EXPECT_EQ(before1.st_size, after1.st_size);
  EXPECT_LT(after0.st_blocks, before0.st_blocks);
  EXPECT_LT(after1.st_blocks, before1.st_blocks);

  Buffer rbuf = device->makeIOBuffer(kSize);
  EXPECT_TRUE(device->read(0, kSize, rbuf.data()));
  for (uint64_t i = 0; i < kSize; i++) {
    const uint8_t expected = i >= kSize - 4 * kStripe ? 0 : 0xef;
    ASSERT_EQ(expected, rbuf.data()[i]) << i;
  }
}

} // namespace facebook::cachelib::navy::tests
