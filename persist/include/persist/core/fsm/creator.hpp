/**
 * fsm/creator.hpp - Persist
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

#ifndef PERSIST_CORE_FSM_CREATOR_HPP
#define PERSIST_CORE_FSM_CREATOR_HPP

#include <persist/core/fsm/base.hpp>

#include <persist/core/fsm/fsl.hpp>

namespace persist {

/**
 * @brief Enumerated list of free space manager types
 *
 */
enum class FSMType { FSL };

/**
 * @brief Factory method to create free space manager of given type.
 *
 * @param fsm_type Type of free space manager to create
 * @returns Unique pointer to created FreeSpaceManager object
 */
template <class BufferManagerType>
static std::unique_ptr<FreeSpaceManager<BufferManagerType>>
CreateFSM(FSMType fsm_type, BufferManagerType *buffer) {
  switch (fsm_type) {
  case FSMType::FSL:
    return std::make_unique<FSList<BufferManagerType>>(buffer);
    break;
  }
}

} // namespace persist

#endif /* PERSIST_CORE_FSM_CREATOR_HPP */
