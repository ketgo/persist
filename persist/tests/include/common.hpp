/**
 * tests/common.hpp - Persist
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
 * @brief The header file contains common definitions and methods used for
 * testing.
 *
 */
#ifndef TESTS_COMMON_HPP
#define TESTS_COMMON_HPP

#include <gtest/gtest.h>

#include <persist/core/page/factory.hpp>

#include "persist/test/simple_page.hpp"

/**
 * @brief Location of the test data.
 */
#define DATA_PATH "/Users/ketan/Projects/persist/build/persist/tests/data"

/**
 * @brief Global testing environment setup.
 *
 */
class Environment : public ::testing::Environment {
public:
  void SetUp() override {
    // Register simple page
    persist::PageFactory::RegisterPage<persist::test::SimplePage>();
  }

  void TearDown() override {
    // UnRegister simple page
    persist::PageFactory::UnRegisterPage<persist::test::SimplePage>();
  }
};

#endif /* TESTS_COMMON_HPP */
