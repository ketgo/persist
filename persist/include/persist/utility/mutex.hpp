/**
 * utility/mutex.hpp - Persist
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

#ifndef UTILITY_MUTEX_HPP
#define UTILITY_MUTEX_HPP

#include <mutex>
#include <shared_mutex>

#include <persist/utility/annotations.hpp>

namespace persist {

/**
 * @brief Mutex Wrapper
 *
 * Wrapper around the standard library mutex types to incorporate thread safety
 * annotations. The standard library types cannot be used directly because it
 * does not provided the required annotations.
 *
 * @example
 *
 *  #include <mutex>
 *
 *  // Wraps the std::mutex with thread safety annotations.
 *  Mutex<std::mutex> mu;
 *  // Wraps the std::recursive_mutex with thread safety annotations.
 *  Mutex<std::recursive_mutex> rmu;
 *
 * @tparam MutexType type of mutex to wrap
 *
 */
template <class MutexType> class CAPABILITY("mutex") Mutex {
private:
  mutable MutexType mutex;

public:
  /**
   * @brief Lock mutex
   *
   */
  void lock() ACQUIRE() { mutex.lock(); }

  /**
   * @brief Tries to lock the mutex. On successful lock acquisition returns
   * true, otherwise returns false.
   *
   */
  bool try_lock() TRY_ACQUIRE(true) { return mutex.try_lock(); }

  /**
   * @brief Unlock mutex
   *
   */
  void unlock() RELEASE() { mutex.unlock(); }

  /**
   * @brief Get reference to the mutex wrapped.
   *
   */
  MutexType &native_handle() { return mutex; }
};

/**
 * @brief Shared Mutex Wrapper
 *
 * Wrapper around the standard library shared mutex types to incorporate thread
 * safety annotations. The standard library types cannot be used directly
 * because it does not provided the required annotations.
 *
 *  @example
 *
 *  #include <shared_mutex>
 *
 *  // Wraps the std::shared_mutex with thread safety annotations.
 *  SharedMutex<std::shared_mutex> smu;
 *
 * @tparam SharedMutexType type of shared mutex to wrap
 */
template <class SharedMutexType> class CAPABILITY("mutex") SharedMutex {
private:
  mutable SharedMutexType mutex;

public:
  /**
   * @brief Lock mutex in exclusive mode
   *
   */
  void lock() ACQUIRE() { mutex.lock(); }

  /**
   * @brief Tries to lock the mutex in exclusive mode. On successful lock
   * acquisition returns true, otherwise returns false.
   *
   */
  bool try_lock() TRY_ACQUIRE(true) { return mutex.try_lock(); }

  /**
   * @brief Unlock mutex from exclusive mode
   *
   */
  void unlock() RELEASE() { mutex.unlock(); }

  /**
   * @brief Lock mutex in shared (reader) mode
   *
   */
  void lock_shared() ACQUIRE_SHARED() { mutex.lock_shared(); }

  /**
   * @brief Tries to lock the mutex in shared mode. On successful lock
   * acquisition returns true, otherwise returns false.
   *
   */
  bool try_lock_shared() TRY_ACQUIRE_SHARED(true) {
    return mutex.try_lock_shared();
  }

  /**
   * @brief Unlock mutex from shared (reader) mode
   *
   */
  void unlock_shared() RELEASE_SHARED() { mutex.unlock_shared(); }

  /**
   * @brief Get reference to the mutex wrapped.
   *
   */
  SharedMutexType &native_handle() { return mutex; }
};

/**
 * @brief Lock Guard Wrapper
 *
 * Wrapper around the standard library class `std::lock_guard` which implement
 * RAII locking.
 *
 * @tparam MutexType type of mutex owned by `std::lock_guard`
 *
 */
template <class MutexType> class SCOPED_CAPABILITY LockGuard {
private:
  std::lock_guard<MutexType> guard;

public:
  LockGuard(MutexType &m) ACQUIRE(m) : guard(m) {}
  ~LockGuard() RELEASE() {}
  std::lock_guard<MutexType> &native_handle() { return guard; }
};

/**
 * @brief Unique Lock Wrapper
 *
 * Wrapper around the standard library class `std::unique_lock` which implement
 * RAII locking.
 *
 * @tparam MutexType type of mutex owned by `std::unique_lock`
 *
 */
template <class MutexType> class SCOPED_CAPABILITY UniqueLock {
private:
  std::unique_lock<MutexType> guard;

public:
  UniqueLock(MutexType &m) ACQUIRE(m) : guard(m) {}
  ~UniqueLock() RELEASE() {}
  std::unique_lock<MutexType> &native_handle() { return guard; }
};

/**
 * @brief Shared Lock Wrapper
 *
 * Wrapper around the standard library class `std::shared_lock` which unlocks
 * the underlying mutex during destructor. Note that the mutex is not locked at
 * construction.
 *
 * @tparam MutexType type of mutex owned by `std::shared_lock`
 *
 */
template <class MutexType> class SCOPED_CAPABILITY SharedLock {
private:
  std::shared_lock<MutexType> guard;

public:
  SharedLock(MutexType &m) : guard(m) {}
  ~SharedLock() RELEASE() {}
  std::shared_lock<MutexType> &native_handle() { return guard; }
};

} // namespace persist

#endif /* UTILITY_MUTEX_HPP */
