/**
 * utility/checksum.hpp - Persist
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

#ifndef PERSIST_UTILITY_CHECKSUM_HPP
#define PERSIST_UTILITY_CHECKSUM_HPP

#include <persist/core/common.hpp>

namespace persist {

// TODO: Implement efficient Alder32 hash function

/**
 * @brief The function object computes the Alder32 hash value for given byte
 * buffer.
 *
 * @param input Span object of the byte buffer for which to hash
 * @returns Alger32 hash value
 */
class Alder32Hash {
private:
  const uint32_t mod = 65521;

public:
  uint32_t operator()(Span input) {
    uint32_t a = 1, b = 0;
    size_t index;

    // Process each byte of the data in order
    for (index = 0; index < input.size; ++index) {
      a = (a + input.start[index]) % mod;
      b = (b + a) % mod;
    }

    return (b << 16) | a;
  }
};

/**
 * @brief The method returns checksum of a byte buffer located by the given span
 * object.
 *
 * @tparam H The hash function for calculating checksum. By default the Alder-32
 * hash is used.
 * @param input Span object of the byte buffer for which to compute checksum.
 * @returns Computed checksum value.
 */
template <typename H = Alder32Hash> Checksum checksum(Span input) {
  return H{}(input);
}

} // namespace persist

#endif /* PERSIST_UTILITY_CHECKSUM_HPP */
