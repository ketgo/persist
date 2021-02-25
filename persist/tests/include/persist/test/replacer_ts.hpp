/**
 * test_replacer_ts.hpp - Persist
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

/**
 * @brief Replacer thread safety test header. The interface provided in this
 * file can be used to test thread safety of custom Replacer implementations.
 *
 */

#ifndef TEST_REPLACER_TS_HPP
#define TEST_REPLACER_TS_HPP

#include <gtest/gtest.h>

namespace persist {
namespace test {

/**
 * @brief Replacer thread safety test fixture.
 *
 * @tparam ReplacerType type of replacer
 */
template <class ReplacerType>
class ReplacerThreadSafetyTestFixture : public testing::Test {};

TYPED_TEST_SUITE_P(ReplacerThreadSafetyTestFixture);

/**
 * @brief Test concurrent call to `getVictum` and `unPin` methods.
 *
 */
TYPED_TEST_P(ReplacerThreadSafetyTestFixture, TestGetVictumUnPin) {
  // Inside a test, refer to TypeParam to get the type parameter.
  TypeParam replace;
}

// Registering all tests
REGISTER_TYPED_TEST_SUITE_P(ReplacerThreadSafetyTestFixture,
                            TestGetVictumUnPin);

} // namespace test

} // namespace persist

#endif /* TEST_REPLACER_TS_HPP */
