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

#ifndef PAGE_FACTORY_HPP
#define PAGE_FACTORY_HPP

#include <functional>
#include <memory>
#include <unordered_map>

#include <persist/core/defs.hpp>
#include <persist/core/page/base.hpp>
#include <persist/core/page/type_header.hpp>

#include <persist/core/page/log_page/log_page.hpp>
#include <persist/core/page/slotted_page/slotted_page.hpp>

namespace persist {
/**
 * @brief Private Page Factory Class
 *
 * The page factory class keeps track of all the different types of user-defiend
 * or in-built page implementions.
 *
 * @tparam T Dummy paramter needed to enable creation of static attributes
 */
template <class T = void> class _PageFactory {
private:
  /**
   * @brief Lookup Table
   *
   * The table maps page type identifer to creator functors.
   *
   */
  typedef std::function<std::unique_ptr<Page>()> PageCreator;
  typedef std::unordered_map<PageTypeId, PageCreator> LookupTable;
  static LookupTable table;

public:
  /**
   * @brief Register a page type to the factory.
   *
   * @tparam PageType The type of page.
   */
  template <class PageType> static void registerPage() {
    PageTypeId page_type_id = PageType().getTypeId();

    // TODO: Throw error if page type already register. The exception should
    // display the type ID value.
    if (table.find(page_type_id) == table.end()) {
      table.insert({page_type_id, std::make_unique<PageType>});
    }
  }

  /**
   * @brief Get a page of specified type identifier. The default constructor is
   * used when creating the page object.
   *
   * @param page_type_id The page type identifier.
   * @returns Unique pointer of base type to the created page object. The user
   * should cast the pointer to that of the desired page type.
   */
  static std::unique_ptr<Page> getPage(PageTypeId page_type_id) {
    return table.at(page_type_id)();
  }
};

/**
 * @brief Initialize lookup table of the page factory.
 *
 * @tparam T Dummy paramter needed to enable creation of static attributes
 */
template <class T>
typename _PageFactory<T>::LookupTable _PageFactory<T>::table = {
    {LogPage().getTypeId(), std::make_unique<LogPage>},
    {SlottedPage().getTypeId(), std::make_unique<SlottedPage>}};

/**
 * @brief Page Factory Class
 *
 * The publically exposed page factory static class. This should be used to
 * access the page factory instead of the template parameterized version.
 */
typedef _PageFactory<> PageFactory;

/**
 * @brief Create an empty page object of specified type.
 *
 * @tparam PageType The type of page to create.
 * @param page_id The page identifier.
 * @param page_size The page size.
 * @returns Unique pointer to the created page.
 */
template <class PageType>
static std::unique_ptr<PageType>
createPage(PageId page_id, uint64_t page_size = DEFAULT_PAGE_SIZE) {
  // The page size is adjusted to incorporate the type header.
  return std::make_unique<PageType>(page_id,
                                    page_size - PageTypeHeader::size());
}

} // namespace persist

#endif /* PAGE_FACTORY_HPP */
