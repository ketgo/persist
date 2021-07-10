/**
 * config.hpp - Persist
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

#ifndef PERSIST_CORE_CONFIG_HPP
#define PERSIST_CORE_CONFIG_HPP

#include <string>

#include <persist/core/defs.hpp>

/**
 * Storage type seperator in connection string
 */
#define STORAGE_TYPE_SEPERATOR "://"

namespace persist {

/**
 * @brief The class parsers a given connection string and exposes the different
 * arguments needed to construct a backens storage object. It assumes that the
 * string has the following schema:
 *
 *      <type>://<host>/<path>/<name>?<arg_1=val_1&arg_2=val_2>
 *
 * where
 * - type [required]: Type of backend storage
 * - host [optional]: Hostname where the storage should be stored
 * - path [required]: Path on host where the storage is located
 * - name [required]: Name of the collection
 * - arg_1..n [optional]: Additional arguments
 * - val_1..n [optional]: Values associated with the additional arguments
 *
 * NOTE: Currently simple parser is implemented which just detects the `type`.
 *
 * TODO:
 *  - Prase arguments like `page_size`.
 *  - Exception for incorrectly formated connection string.
 */
class ConnectionString {
public:
  std::string raw;
  std::string type;
  std::string path;
  std::string extension;
  std::string collection_name;
  size_t page_size;

  /**
   * @brief Construct a new Connection String object
   *
   * @param connection_string Connection string as string type.
   */
  ConnectionString(const std::string &connection_string)
      : raw(connection_string), page_size(DEFAULT_PAGE_SIZE) {
    std::string seperator = STORAGE_TYPE_SEPERATOR;
    std::string::size_type loc = raw.find(seperator);
    type = raw.substr(0, loc);
    path = raw.substr(loc + seperator.size());
  }

  /**
   * @brief Construct a new Connection String object
   *
   * @param connection_string Connection string as constant char array.
   */
  ConnectionString(const char *connection_string)
      : raw(connection_string), page_size(DEFAULT_PAGE_SIZE) {
    std::string seperator = STORAGE_TYPE_SEPERATOR;
    std::string::size_type loc = raw.find(seperator);
    type = raw.substr(0, loc);
    path = raw.substr(loc + seperator.size());
  }

  /**
   * @brief Construct a new Connection String object
   *
   * @param connection_string Connection string as string type.
   * @param extension Storage extension string.
   */
  ConnectionString(const std::string &connection_string, const char *extension)
      : raw(connection_string), extension(extension),
        page_size(DEFAULT_PAGE_SIZE) {
    std::string seperator = STORAGE_TYPE_SEPERATOR;
    std::string::size_type loc = raw.find(seperator);
    type = raw.substr(0, loc);
    path = raw.substr(loc + seperator.size()) + extension;
  }

  /**
   * @brief Construct a new Connection String object
   *
   * @param connection_string Connection string as constant char array.
   * @param extension Storage extension string.
   */
  ConnectionString(const char *connection_string, const char *extension)
      : raw(connection_string), extension(extension),
        page_size(DEFAULT_PAGE_SIZE) {
    std::string seperator = STORAGE_TYPE_SEPERATOR;
    std::string::size_type loc = raw.find(seperator);
    type = raw.substr(0, loc);
    path = raw.substr(loc + seperator.size()) + extension;
  }
};

/**
 * @brief Class containing collection specific configurations for package core
 * components. The class parameters are parsed from user provided connection
 * string or configuration file.
 *
 */
class CollectionConfig {
public:
  /**
   * @brief Construct a new Collection Config object
   *
   * @param connection_string
   */
  CollectionConfig(const std::string &connection_string) {}

  /**
   * @brief Construct a new Collection Config object
   *
   * @param connection_string
   */
  CollectionConfig(const char *connection_string) {}
};

} // namespace persist

#endif /* PERSIST_CORE_CONFIG_HPP */
