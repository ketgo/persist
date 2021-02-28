/**
 * test_factory.cpp - Persist
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
 * @brief Page Factory Unit Test
 *
 */

#include <gtest/gtest.h>

#include <memory>

/**
 * Enabled intrusive testing
 */
#define PERSIST_INTRUSIVE_TESTING

#include <persist/core/page/factory.hpp>

#include "persist/test/simple_page.hpp"

using namespace persist;
using namespace persist::test;

TEST(PageFactoryTestFixture, TestGet) {
  auto page = PageFactory::getPage(1);
  auto *ptr = page.get();
  std::string className = typeid(*ptr).name();
  ASSERT_TRUE(className.find("LogPage") != std::string::npos);
}

TEST(PageFactoryTestFixture, TestRegister) {
  PageFactory::registerPage<SimplePage>();
  auto page = PageFactory::getPage(SimplePage().getTypeId());
  auto *ptr = page.get();
  std::string className = typeid(*ptr).name();
  ASSERT_TRUE(className.find("SimplePage") != std::string::npos);
}
