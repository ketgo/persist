/**
 * page.hpp - Persist
 *
 * Copyright 2021 Ketan Goyal
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef PERSIST_TEST_MOCKS_PAGE_HPP
#define PERSIST_TEST_MOCKS_PAGE_HPP

#include <gmock/gmock.h>

#include <persist/core/page/base.hpp>

using ::testing::_;
using ::testing::Invoke;

namespace persist {
namespace test {

/**
 * @brief Fake Page
 *
 */
class FakePage : public Page {
private:
  PageId page_id;
  size_t page_size;

public:
  FakePage(PageId page_id = 1, size_t page_size = DEFAULT_PAGE_SIZE)
      : page_id(page_id), page_size(page_size) {}

  PageTypeId GetTypeId() const override { return MINIMUM_PAGE_TYPE_ID; }
  const PageId &GetId() const override { return page_id; }
  size_t GetFreeSpaceSize(Operation operation) const override {
    return DEFAULT_PAGE_SIZE;
  }
  void Load(Span input) override {}
  void Dump(Span output) override {}
};

/**
 * @brief Mock Page
 *
 */
class MockPage : public Page {
public:
  MOCK_METHOD(PageTypeId, GetTypeId, (), (const override));
  MOCK_METHOD(const PageId &, GetId, (), (const override));
  MOCK_METHOD(size_t, GetFreeSpaceSize, (Operation), (const override));
  MOCK_METHOD(void, Load, (Span), (override));
  MOCK_METHOD(void, Dump, (Span), (override));

  void UseFake() {
    ON_CALL(*this, GetTypeId())
        .WillByDefault(Invoke(&fake, &FakePage::GetTypeId));

    ON_CALL(*this, GetId()).WillByDefault(Invoke(&fake, &FakePage::GetId));

    ON_CALL(*this, GetFreeSpaceSize(_))
        .WillByDefault(Invoke(&fake, &FakePage::GetFreeSpaceSize));

    ON_CALL(*this, Load(_)).WillByDefault(Invoke(&fake, &FakePage::Load));

    ON_CALL(*this, Dump(_)).WillByDefault(Invoke(&fake, &FakePage::Dump));
  }

private:
  FakePage fake;
};

} // namespace test
} // namespace persist

#endif /* PERSIST_TEST_MOCKS_PAGE_HPP */