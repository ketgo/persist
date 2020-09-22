/**
 * common.hpp - Persist
 *
 * Copyright 2020 Ketan Goyal
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
 * This header file contains common Classes and methods used in the package.
 */

#ifndef COMMON_HPP
#define COMMON_HPP

#include <vector>

namespace persist {
/**
 * Abstract Base Serializable class
 */
class Serializable {
public:
  virtual ~Serializable() {} //<- Virtual Destructor

  /**
   * Load object from byte string
   *
   * @param input input buffer to load
   */
  virtual void load(std::vector<uint8_t> &input) = 0;

  /**
   * Dump object as byte string
   *
   * @param output output buffer to dump
   */
  virtual void dump(std::vector<uint8_t> &output) = 0;
};
} // namespace persist

#endif /* COMMON_HPP */