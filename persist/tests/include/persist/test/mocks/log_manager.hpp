/**
 * log_manager.hpp - Persist
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

#ifndef PERSIST_TEST_MOCKS_LOG_MANAGER_HPP
#define PERSIST_TEST_MOCKS_LOG_MANAGER_HPP

#include <gmock/gmock.h>

#include <persist/core/log/log_manager.hpp>

using ::testing::_;
using ::testing::Invoke;

namespace persist {
namespace test {

/**
 * @brief Fake Log Manager
 *
 */
class FakeLogManager : public LogManager {
public:
  FakeLogManager() : LogManager(nullptr) {}

  void start() {}
  void stop() {}
  LogRecord::Location add(LogRecord &) { return LogRecord::Location(1, 1); }
  std::unique_ptr<LogRecord> get(LogRecord::Location) {
    return std::make_unique<LogRecord>();
  }
  void flush() {}
};

/**
 * @brief Mock Log Manager
 *
 */
class MockLogManager : public LogManager {
public:
  MockLogManager() : LogManager(nullptr) {}

  MOCK_METHOD(void, start, (), ());
  MOCK_METHOD(void, stop, (), ());
  MOCK_METHOD(LogRecord::Location, add, (LogRecord &), ());
  MOCK_METHOD(std::unique_ptr<LogRecord>, get, (LogRecord::Location), ());
  MOCK_METHOD(void, flush, (), ());

  void useFake() {
    ON_CALL(*this, start())
        .WillByDefault(Invoke(&fake, &FakeLogManager::start));

    ON_CALL(*this, stop()).WillByDefault(Invoke(&fake, &FakeLogManager::stop));

    ON_CALL(*this, add(_)).WillByDefault(Invoke(&fake, &FakeLogManager::add));

    ON_CALL(*this, get(_)).WillByDefault(Invoke(&fake, &FakeLogManager::get));

    ON_CALL(*this, flush())
        .WillByDefault(Invoke(&fake, &FakeLogManager::flush));
  }

private:
  FakeLogManager fake;
};

} // namespace test
} // namespace persist

#endif /* PERSIST_TEST_MOCKS_LOG_MANAGER_HPP */
