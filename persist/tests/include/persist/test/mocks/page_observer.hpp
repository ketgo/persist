/**
 * page_observer.hpp - Persist
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

#ifndef PERSIST_TEST_MOCKS_PAGE_OBSERVER_HPP
#define PERSIST_TEST_MOCKS_PAGE_OBSERVER_HPP

#include <gmock/gmock.h>

#include <persist/core/page/base.hpp>

using ::testing::_;
using ::testing::Invoke;

namespace persist {
namespace test {

/**
 * @brief Page Observer Mock
 *
 */
class MockPageObserver : public PageObserver {
public:
  MOCK_METHOD(void, HandleModifiedPage, (const Page &), (override));
};

} // namespace test
} // namespace persist

#endif /* PERSIST_TEST_MOCKS_PAGE_OBSERVER_HPP */
