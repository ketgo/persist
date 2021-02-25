/**
 * utility/annotations.hpp - Persist
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
 * @brief Thread Annotations
 *
 * The file contains annotations used for static thread safety analysis. They
 * allow developers to document the locking policies of multi-threaded code. The
 * annotations can also help program analysis tools to identify potential thread
 * safety issues.
 *
 * These annotations are implemented using compiler attributes. Using the macros
 * defined here instead of raw attributes allow for portability and future
 * compatibility.
 *
 * NOTE: The annotations currently only work for clang compiler. Since gcc does
 * not support static thread safety analysis, the annotations have no effect
 * when compiling with gcc.
 */

#ifndef UTILITY_ANNOTATIONS_HPP
#define UTILITY_ANNOTATIONS_HPP

#if defined(__clang__)
#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
#define THREAD_ANNOTATION_ATTRIBUTE__(x) // no-op
#endif

/**
 * @brief CAPABILITY
 *
 * Documents that a class has locking capability. The macro allows the user to
 * set thread safety analysis for custom mutex classes.
 *
 * @example
 *
 *  class CAPABILITY("mutex") Mutex {
 *    ...
 *  };
 *
 */
#define CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(capability(x))

/**
 * @brief SCOPED_CAPABILITY
 *
 * Documents that a class does RAII locking (such as the `std::unique_lock`
 * class). The constructor should use `ACQUIRE()` to specify the mutex that is
 * acquired, and the destructor should use `RELEASE()` with no arguments; the
 * analysis will assume that the destructor unlocks whatever the constructor
 * locked.
 *
 * @example
 *
 *  class SCOPED_CAPABILITY LockGuard {
 *    ...
 *  };
 *
 */
#define SCOPED_CAPABILITY THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

/**
 * @brief GUARDED_BY()
 *
 * Documents if a shared field or global variable needs to be protected by a
 * mutex. GUARDED_BY() allows the user to specify a particular mutex that should
 * be held when accessing the annotated variable.
 *
 * Although this annotation (and PT_GUARDED_BY, below) cannot be applied to
 * local variables, a local variable and its associated mutex can often be
 * combined into a small class or struct, thereby allowing the annotation.
 *
 * @example:
 *
 *  class Foo {
 *      Mutex mu_;
 *      int p1_ GUARDED_BY(mu_);
 *      ...
 *  };
 *
 */
#define GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

/**
 * @brief PT_GUARDED_BY()
 *
 * Documents if the memory location pointed to by a pointer should be guarded by
 * a mutex when dereferencing the pointer.
 *
 * @example:
 *
 *  class Foo {
 *      Mutex mu_;
 *      int *p1_ PT_GUARDED_BY(mu_);
 *      ...
 *  };
 *
 * Note that a pointer variable to a shared memory location could itself be a
 * shared variable.
 *
 * @example:
 *
 *  // `q_`, guarded by `mu1_`, points to a shared memory location that is
 *  // guarded by `mu2_`:
 *  int *q_ GUARDED_BY(mu1_) PT_GUARDED_BY(mu2_);
 *
 */
#define PT_GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))

/**
 * @brief ACQUIRED_AFTER() / ACQUIRED_BEFORE()
 *
 * Documents the acquisition order between locks that can be held simultaneously
 * by a thread. For any two locks that need to be annotated to establish an
 * acquisition order, only one of them needs the annotation. (i.e. You don't
 * have to annotate both locks with both ACQUIRED_AFTER and ACQUIRED_BEFORE.)
 *
 * As with GUARDED_BY, this is only applicable to mutexes that are shared fields
 * or global variables.
 *
 * @example:
 *
 *  Mutex m1_;
 *  Mutex m2_ ACQUIRED_AFTER(m1_);
 *
 */
#define ACQUIRED_AFTER(...)                                                    \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))
#define ACQUIRED_BEFORE(...)                                                   \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

/**
 * @brief REQUIRES() / SHARED_LOCKS_REQUIRED()
 *
 * Documents a function that expects a mutex to be held prior to entry. The
 * mutex is expected to be held both on entry to, and exit from, the function.
 *
 * An exclusive lock allows read-write access to the guarded data member(s), and
 * only one thread can acquire a lock exclusively at any one time. A shared lock
 * allows read-only access, and any number of threads can acquire a shared lock
 * concurrently.
 *
 * Generally, non-const methods should be annotated with REQUIRES, while const
 * methods should be annotated with REQUIRES_SHARED.
 *
 * @example:
 *
 *  Mutex mu1, mu2;
 *  int a GUARDED_BY(mu1);
 *  int b GUARDED_BY(mu2);
 *
 *  void foo() REQUIRES(mu1, mu2) { ... }
 *  void bar() const REQUIRES_SHARED(mu1, mu2) { ... }
 *
 *  void test() {
 *    mu1.lock();
 *    mu2.lock();
 *    foo();
 *    bar();
 *    mu1.unlock();
 *    mu2.unlock();
 *  }
 *
 */
#define REQUIRES(...)                                                          \
  THREAD_ANNOTATION_ATTRIBUTE__(requires_capability(__VA_ARGS__))
#define REQUIRES_SHARED(...)                                                   \
  THREAD_ANNOTATION_ATTRIBUTE__(requires_shared_capability(__VA_ARGS__))

/**
 * @brief ACQUIRE()/ACQUIRE_SHARED()
 *
 * Documents functions that acquire a lock in the body of a function, and do not
 * release it. The lock can be exclusive or shared (reader) respectively.
 *
 */
#define ACQUIRE(...)                                                           \
  THREAD_ANNOTATION_ATTRIBUTE__(acquire_capability(__VA_ARGS__))
#define ACQUIRE_SHARED(...)                                                    \
  THREAD_ANNOTATION_ATTRIBUTE__(acquire_shared_capability(__VA_ARGS__))

/**
 * @brief RELEASE()/RELEASE_SHARED()/RELEASE_GENERIC()
 *
 * Documents functions expect a lock (exclusively for RELEASE, shared for
 * RELEASE_SHARED, exclusively or shared for RELEASE_GENERIC) to be held on
 * entry to the function, and release it in the body of the function.
 *
 */
#define RELEASE(...)                                                           \
  THREAD_ANNOTATION_ATTRIBUTE__(release_capability(__VA_ARGS__))
#define RELEASE_SHARED(...)                                                    \
  THREAD_ANNOTATION_ATTRIBUTE__(release_shared_capability(__VA_ARGS__))
#define RELEASE_GENERIC(...)                                                   \
  THREAD_ANNOTATION_ATTRIBUTE__(release_generic_capability(__VA_ARGS__))

/**
 * @brief TRY_ACQUIRE() / TRY_ACQUIRE_SHARED()
 *
 * Documents functions that try to acquire a lock, and return success or failure
 * (or a non-boolean value that can be interpreted as a boolean). The first
 * argument should be `true` for functions that return `true` on success, or
 * `false` for functions that return `false` on success. The second argument
 * specifies the mutex that is locked on success. If unspecified, this mutex is
 * assumed to be `this`.
 *
 */
#define TRY_ACQUIRE(...)                                                       \
  THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_capability(__VA_ARGS__))
#define TRY_ACQUIRE_SHARED(...)                                                \
  THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_shared_capability(__VA_ARGS__))

/**
 * @brief EXCLUDES()
 *
 * Documents that the locks acquired in the body of the function. These locks
 * cannot be held when calling this function, e.g. non-reentrant locks.
 *
 */
#define EXCLUDES(...) THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

/**
 * @brief ASSERT_CAPABILITY() / ASSERT_SHARED_CAPABILITY()
 *
 * Documents functions that dynamically check to see if a lock is held, and fail
 * if it is not held.
 *
 */
#define ASSERT_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(assert_capability(x))
#define ASSERT_SHARED_CAPABILITY(x)                                            \
  THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_capability(x))

/**
 * @brief RETURN_CAPABILITY()
 *
 * Documents a function that returns a mutex without acquiring it.  For example,
 * a public getter method that returns a pointer to a private mutex should be
 * annotated with RETURN_CAPABILITY.
 *
 */
#define RETURN_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

/**
 * @brief NO_THREAD_SAFETY_ANALYSIS
 *
 * Turns off thread safety checking within the body of a particular function.
 * This annotation is used to mark functions that are known to be correct, but
 * the locking behavior is more complicated than the analyzer can handle.
 *
 */
#define NO_THREAD_SAFETY_ANALYSIS                                              \
  THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

//------------------------------------------------------------------------------
// Tool-Supplied Annotations
//------------------------------------------------------------------------------

/**
 * TS_UNCHECKED should be placed around lock expressions that are not valid C++
 * syntax, but which are present for documentation purposes.  These annotations
 * will be ignored by the analysis.
 *
 */
#define TS_UNCHECKED(x) ""

/**
 * TS_FIXME is used to mark lock expressions that are not valid C++ syntax. It
 * is used by automated tools to mark and disable invalid expressions. The
 * annotation should either be fixed, or changed to TS_UNCHECKED.
 *
 */
#define TS_FIXME(x) ""

/**
 * Like NO_THREAD_SAFETY_ANALYSIS, this turns off checking within the body of a
 * particular function.  However, this attribute is used to mark functions that
 * are incorrect and need to be fixed.  It is used by automated tools to avoid
 * breaking the build when the analysis is updated. Code owners are expected to
 * eventually fix the routine.
 *
 */
#define NO_THREAD_SAFETY_ANALYSIS_FIXME NO_THREAD_SAFETY_ANALYSIS

/**
 * Similar to NO_THREAD_SAFETY_ANALYSIS_FIXME, this macro marks a GUARDED_BY
 * annotation that needs to be fixed, because it is producing thread safety
 * warning.  It disables the GUARDED_BY.
 *
 */
#define GUARDED_BY_FIXME(x)

/**
 * Disables warnings for a single read operation.  This can be used to avoid
 * warnings when it is known that the read is not actually involved in a race,
 * but the compiler cannot confirm that.
 *
 */
#define TS_UNCHECKED_READ(x) thread_safety_analysis::ts_unchecked_read(x)

namespace thread_safety_analysis {

/**
 * Takes a const reference to a guarded data member, and returns an unguarded
 * const reference.
 */
template <typename T>
inline const T &ts_unchecked_read(const T &v) NO_THREAD_SAFETY_ANALYSIS {
  return v;
}

/**
 * Takes a reference to a guarded data member, and returns an unguarded
 * reference.
 */
template <typename T>
inline T &ts_unchecked_read(T &v) NO_THREAD_SAFETY_ANALYSIS {
  return v;
}

} // namespace thread_safety_analysis

#endif /* UTILITY_ANNOTATIONS_HPP */
