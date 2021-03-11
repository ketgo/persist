/**
 * replacer/factory.hpp - Persist
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

#ifndef PERSIST_CORE_BUFFER_REPLACER_FACTORY_HPP
#define PERSIST_CORE_BUFFER_REPLACER_FACTORY_HPP

#include <persist/core/buffer/replacer/base.hpp>
#include <persist/core/buffer/replacer/lru_replacer.hpp>

namespace persist {

/**
 * @brief Enumerated list of replacer types
 *
 */
enum class ReplacerType { LRU };

/**
 * @brief Factory method to create replacer of given type.
 *
 * @param replacer_type type of replacer to create
 * @returns unique pointer to created replacer object
 */
static std::unique_ptr<Replacer> CreateReplacer(ReplacerType replacer_type) {
  switch (replacer_type) {
  case ReplacerType::LRU:
    return std::make_unique<LRUReplacer>();
    break;
  }
}

} // namespace persist

#endif /* PERSIST_CORE_BUFFER_REPLACER_FACTORY_HPP */
