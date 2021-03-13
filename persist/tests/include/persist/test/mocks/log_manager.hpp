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

  void Start() {}
  void Stop() {}
  LogRecord::Location Add(LogRecord &) { return LogRecord::Location(1, 1); }
  std::unique_ptr<LogRecord> Get(LogRecord::Location) {
    return std::make_unique<LogRecord>();
  }
  void Flush() {}
};

/**
 * @brief Mock Log Manager
 *
 */
class MockLogManager : public LogManager {
public:
  MockLogManager() : LogManager(nullptr) {}

  MOCK_METHOD(void, Start, (), ());
  MOCK_METHOD(void, Stop, (), ());
  MOCK_METHOD(LogRecord::Location, Add, (LogRecord &), ());
  MOCK_METHOD(std::unique_ptr<LogRecord>, Get, (LogRecord::Location), ());
  MOCK_METHOD(void, Flush, (), ());

  void UseFake() {
    ON_CALL(*this, Start())
        .WillByDefault(Invoke(&fake, &FakeLogManager::Start));

    ON_CALL(*this, Stop()).WillByDefault(Invoke(&fake, &FakeLogManager::Stop));

    ON_CALL(*this, Add(_)).WillByDefault(Invoke(&fake, &FakeLogManager::Add));

    ON_CALL(*this, Get(_)).WillByDefault(Invoke(&fake, &FakeLogManager::Get));

    ON_CALL(*this, Flush())
        .WillByDefault(Invoke(&fake, &FakeLogManager::Flush));
  }

private:
  FakeLogManager fake;
};

} // namespace test
} // namespace persist

#endif /* PERSIST_TEST_MOCKS_LOG_MANAGER_HPP */
