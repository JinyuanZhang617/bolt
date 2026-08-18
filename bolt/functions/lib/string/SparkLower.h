/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <memory>

#include <folly/CPortability.h>
#include <folly/small_vector.h>
#include <unicode/brkiter.h>
#include <unicode/locid.h>
#include <unicode/uchar.h>
#include <unicode/unistr.h>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::functions::stringCore::spark {

FOLLY_ALWAYS_INLINE bool isJavaCased(UChar32 codePoint) {
  const auto category = u_charType(codePoint);
  return category == U_LOWERCASE_LETTER || category == U_UPPERCASE_LETTER ||
      category == U_TITLECASE_LETTER ||
      (codePoint >= 0x02B0 && codePoint <= 0x02B8) ||
      (codePoint >= 0x02C0 && codePoint <= 0x02C1) ||
      (codePoint >= 0x02E0 && codePoint <= 0x02E4) || codePoint == 0x0345 ||
      codePoint == 0x037A || (codePoint >= 0x1D2C && codePoint <= 0x1D61) ||
      (codePoint >= 0x2160 && codePoint <= 0x217F) ||
      (codePoint >= 0x24B6 && codePoint <= 0x24E9);
}

FOLLY_ALWAYS_INLINE bool isJavaWordBoundary(
    const icu::UnicodeString& input,
    int32_t offset,
    icu::BreakIterator& breakIterator) {
  if (!breakIterator.isBoundary(offset)) {
    return false;
  }
  if (offset == 0 || offset == input.length()) {
    return true;
  }

  const auto bridgesBoundary = [](UChar32 codePoint) {
    return codePoint == '"' || codePoint == '-';
  };
  return !bridgesBoundary(input.char32At(offset - 1)) &&
      !bridgesBoundary(input.char32At(offset));
}

FOLLY_ALWAYS_INLINE bool isJavaFinalSigma(
    const icu::UnicodeString& input,
    int32_t sigmaOffset,
    icu::BreakIterator& breakIterator) {
  auto offset = sigmaOffset;
  while (offset >= 0 && !isJavaWordBoundary(input, offset, breakIterator)) {
    const auto codePoint = input.char32At(offset - 1);
    if (isJavaCased(codePoint)) {
      offset = sigmaOffset + 1;
      while (offset < input.length() &&
             !isJavaWordBoundary(input, offset, breakIterator)) {
        const auto followingCodePoint = input.char32At(offset);
        if (isJavaCased(followingCodePoint)) {
          return false;
        }
        offset += U16_LENGTH(followingCodePoint);
      }
      return true;
    }
    offset -= U16_LENGTH(codePoint);
  }
  return false;
}

/// Holds one word break iterator per thread. BreakIterator::setText retains a
/// reference to its input, hence the iterator is rebound to emptyText_ between
/// calls before the caller's UnicodeString is modified or destroyed.
class SparkBreakIteratorHolder {
 public:
  SparkBreakIteratorHolder() {
    UErrorCode status = U_ZERO_ERROR;
    const auto& locale = icu::Locale::getRoot();
    breakIterator_.reset(
        icu::BreakIterator::createWordInstance(locale, status));
    BOLT_USER_CHECK(
        U_SUCCESS(status) && breakIterator_ != nullptr,
        "Failed to create BreakIterator: {}, for locale: {}, Data Path: {}",
        u_errorName(status),
        locale.getName(),
        u_getDataDirectory());
    breakIterator_->setText(emptyText_);
  }

  icu::BreakIterator& breakIterator() {
    return *breakIterator_;
  }

  const icu::UnicodeString& emptyText() const {
    return emptyText_;
  }

 private:
  // Members are destroyed in reverse declaration order. Keep emptyText_ alive
  // until after breakIterator_ releases its retained text reference.
  icu::UnicodeString emptyText_;
  std::unique_ptr<icu::BreakIterator> breakIterator_;
};

FOLLY_ALWAYS_INLINE SparkBreakIteratorHolder& sparkBreakIteratorHolder() {
  static thread_local SparkBreakIteratorHolder holder;
  return holder;
}

class ScopedBreakIteratorText {
 public:
  ScopedBreakIteratorText(
      SparkBreakIteratorHolder& holder,
      const icu::UnicodeString& input)
      : holder_(holder) {
    holder_.breakIterator().setText(input);
  }

  ~ScopedBreakIteratorText() {
    holder_.breakIterator().setText(holder_.emptyText());
  }

  ScopedBreakIteratorText(const ScopedBreakIteratorText&) = delete;
  ScopedBreakIteratorText& operator=(const ScopedBreakIteratorText&) = delete;

 private:
  SparkBreakIteratorHolder& holder_;
};

FOLLY_ALWAYS_INLINE void adjustJavaSigmaInPlace(
    icu::UnicodeString& input,
    int32_t sigmaOffset) {
  struct Replacement {
    int32_t offset;
    char16_t codePoint;
  };

  folly::small_vector<Replacement, 4> replacements;
  {
    auto& holder = sparkBreakIteratorHolder();
    ScopedBreakIteratorText scopedText(holder, input);
    auto& breakIterator = holder.breakIterator();
    do {
      replacements.push_back(
          {sigmaOffset,
           isJavaFinalSigma(input, sigmaOffset, breakIterator)
               ? char16_t{0x03C2}
               : char16_t{0x03C3}});
      sigmaOffset = input.indexOf(0x03A3, sigmaOffset + 1);
    } while (sigmaOffset >= 0);
  }

  // BreakIterator no longer references input. It is now safe to mutate it.
  for (const auto& replacement : replacements) {
    input.setCharAt(replacement.offset, replacement.codePoint);
  }
}

} // namespace bytedance::bolt::functions::stringCore::spark
