/**
 * page/factory.hpp - Persist
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

#ifndef PERSIST_CORE_PAGE_FACTORY_HPP
#define PERSIST_CORE_PAGE_FACTORY_HPP

#include <functional>
#include <memory>
#include <unordered_map>

#include <persist/core/defs.hpp>
#include <persist/core/page/base.hpp>
#include <persist/core/page/simple_page_a.hpp>
#include <persist/core/page/simple_page_b.hpp>

namespace persist {

namespace Page {

template <class T = void> class _Factory {
private:
  typedef std::function<std::unique_ptr<PageBase>()> PageCreator;
  typedef std::unordered_map<PageTypeId, PageCreator> LookupTable;
  static LookupTable table;

public:
  template <class PageType> static void Register() {
    PageTypeId page_type_id = PageType().GetPageTypeId();
    if (table.find(page_type_id) == table.end()) {
      table.insert({page_type_id, std::make_unique<PageType>});
    }
  }

  static const PageCreator &Get(PageTypeId page_type_id) {
    return table.at(page_type_id);
  }
};

template <class T>
typename _Factory<T>::LookupTable _Factory<T>::table = {
    {0, std::make_unique<SimplePageA>}, {1, std::make_unique<SimplePageB>}};

typedef _Factory<> Factory;

static void Dump(PageBase &page, Span output) {
  // 1. Get page type id from page object
  // 2. Dump the page type id to the output buffer
  // 3. Call the `dump` method of the page object to dump the page to rest of
  // the buffer
}

static std::unique_ptr<PageBase> Load(Span input) {
  // 1. Get PageTypeId from input buffer
  // 2. Use PageFactory to create empty Page object
  // 3. Call the `load` method of the page object to load rest of the buffer
}

/**
 * @brief Create an empty page object of specified type.
 *
 * @tparam PageType The type of page to create.
 * @param page_id The page identifier.
 * @param page_size The page size.
 * @returns Unique pointer to the created page.
 */
template <class PageType>
static std::unique_ptr<PageType> Create(PageId page_id, uint64_t page_size) {
  // 1. Adjust the page size for page type identifer
  // 2. Create and return unique pointer of specified page passing the page
  // identifer and adjusted size as constructor arguments.
}

} // namespace Page

} // namespace persist

#endif /* PERSIST_CORE_PAGE_FACTORY_HPP */
