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

#include "bolt/functions/lib/string/SparkLower.h"

#include <algorithm>
#include <array>
#include <cstdint>

#include <unicode/utf16.h>

namespace bytedance::bolt::functions::stringCore::spark {
namespace {

constexpr UChar32 kCharacterIteratorDone = 0xFFFF;
constexpr int32_t kBreakIteratorDone = -1;
constexpr char16_t kSmallSigma = u'\u03C3';
constexpr char16_t kFinalSigma = u'\u03C2';
constexpr int8_t kIgnoreCategory = -1;
constexpr uint16_t kStartState = 1;
constexpr uint16_t kStopState = 0;

struct JavaCasedRange {
  UChar32 first;
  UChar32 last;
};

#include "bolt/functions/lib/string/OpenJdkWordBreakData.inc"

static_assert(
    kJavaWordForwardTransitions.size() ==
    kJavaWordForwardStateCount * kJavaWordCategoryCount);
static_assert(
    kJavaWordBackwardTransitions.size() ==
    kJavaWordBackwardStateCount * kJavaWordCategoryCount);
static_assert(kJavaWordEndStates.size() == kJavaWordForwardStateCount);

FOLLY_ALWAYS_INLINE int32_t
nextCodePointOffset(const icu::UnicodeString& input, int32_t offset) {
  return offset + U16_LENGTH(input.char32At(offset));
}

FOLLY_ALWAYS_INLINE UChar32
javaCurrent(const icu::UnicodeString& input, int32_t offset) {
  if (offset >= input.length()) {
    return kCharacterIteratorDone;
  }
  UChar32 codePoint;
  U16_NEXT(input.getBuffer(), offset, input.length(), codePoint);
  return codePoint;
}

FOLLY_ALWAYS_INLINE int32_t
javaNextOffset(const icu::UnicodeString& input, int32_t offset) {
  if (offset >= input.length()) {
    return input.length();
  }
  U16_FWD_1(input.getBuffer(), offset, input.length());
  return offset;
}

struct PreviousCharacter {
  int32_t offset;
  UChar32 codePoint;
};

FOLLY_ALWAYS_INLINE PreviousCharacter
javaPrevious(const icu::UnicodeString& input, int32_t offset) {
  if (offset <= 0) {
    return {0, kCharacterIteratorDone};
  }
  UChar32 codePoint;
  U16_PREV(input.getBuffer(), 0, offset, codePoint);
  return {offset, codePoint};
}

FOLLY_ALWAYS_INLINE int32_t
previousCodePointOffset(const icu::UnicodeString& input, int32_t offset) {
  return input.moveIndex32(offset, -1);
}

FOLLY_ALWAYS_INLINE int8_t lookupJavaWordCategory(UChar32 codePoint) {
  if (codePoint < 0x10000) {
    const auto index = kJavaWordBmpIndices[codePoint >> 7] + (codePoint & 0x7f);
    return kJavaWordBmpCategories[index];
  }

  size_t low = 0;
  size_t high = kJavaWordSupplementaryCategories.size() - 1;
  while (true) {
    const auto middle = (low + high) / 2;
    const auto start = kJavaWordSupplementaryCategories[middle] >> 8;
    const auto limit = kJavaWordSupplementaryCategories[middle + 1] >> 8;
    if (static_cast<uint32_t>(codePoint) < start) {
      high = middle;
    } else if (static_cast<uint32_t>(codePoint) >= limit) {
      low = middle;
    } else {
      const auto category =
          static_cast<uint8_t>(kJavaWordSupplementaryCategories[middle] & 0xff);
      return category == 0xff ? kIgnoreCategory : static_cast<int8_t>(category);
    }
  }
}

FOLLY_ALWAYS_INLINE uint16_t
lookupForwardState(uint16_t state, int8_t category) {
  return kJavaWordForwardTransitions[state * kJavaWordCategoryCount + category];
}

FOLLY_ALWAYS_INLINE uint16_t
lookupBackwardState(uint16_t state, int8_t category) {
  return kJavaWordBackwardTransitions
      [state * kJavaWordCategoryCount + category];
}

FOLLY_ALWAYS_INLINE bool isJavaCased(UChar32 codePoint) {
  const auto* begin = kJavaCasedRanges.data();
  const auto* end = begin + kJavaCasedRanges.size();
  const auto* range = std::upper_bound(
      begin,
      end,
      codePoint,
      [](UChar32 value, const JavaCasedRange& candidate) {
        return value < candidate.first;
      });
  return range != begin && codePoint <= (range - 1)->last;
}

/// Specialized port of OpenJDK RuleBasedBreakIterator.handlePrevious(). It
/// returns a guaranteed boundary at or before offset, but not necessarily the
/// closest one. GenJavaWordBreakData rejects data with lookahead states or
/// additional data; those invariants are required by the specialized forward
/// evaluator below.
struct SafeBoundaryResult {
  int32_t offset;
  bool needsUtf16Fallback;
};

SafeBoundaryResult findSafeBoundaryBefore(
    const icu::UnicodeString& input,
    int32_t offset) {
  if (offset <= 0) {
    return {0, false};
  }

  uint16_t state = kStartState;
  int8_t category = 0;
  int8_t lastCategory = 0;
  auto currentOffset = offset;

  while (currentOffset > 0 && state != kStopState) {
    currentOffset = previousCodePointOffset(input, currentOffset);
    const auto codePoint = input.char32At(currentOffset);
    // java.text.CharacterIterator uses U+FFFF as its DONE sentinel, including
    // when that value occurs literally in the input.
    if (codePoint == kCharacterIteratorDone) {
      return {currentOffset, false};
    }
    if (U16_LENGTH(codePoint) == 2) {
      return {0, true};
    }
    lastCategory = category;
    category = lookupJavaWordCategory(codePoint);
    if (category != kIgnoreCategory) {
      state = lookupBackwardState(state, category);
    }
  }

  if (state == kStopState) {
    // handlePrevious() moves one code point too far while detecting the stop
    // transition. Ignore characters require only one compensating step.
    if (lastCategory != kIgnoreCategory) {
      currentOffset = nextCodePointOffset(input, currentOffset);
    }
  }
  return {currentOffset, false};
}

/// Ports RuleBasedBreakIterator.handlePrevious() for an arbitrary UTF-16
/// code-unit offset, including an offset inside a surrogate pair.
int32_t findJavaSafeBoundary(const icu::UnicodeString& input, int32_t offset) {
  uint16_t state = kStartState;
  int8_t category = 0;
  int8_t lastCategory = 0;
  auto currentOffset = offset;
  auto codePoint = javaCurrent(input, currentOffset);

  while (codePoint != kCharacterIteratorDone && state != kStopState) {
    lastCategory = category;
    category = lookupJavaWordCategory(codePoint);
    if (category != kIgnoreCategory) {
      state = lookupBackwardState(state, category);
    }

    const auto previous = javaPrevious(input, currentOffset);
    currentOffset = previous.offset;
    codePoint = previous.codePoint;
  }

  if (codePoint != kCharacterIteratorDone) {
    currentOffset = javaNextOffset(input, currentOffset);
    if (lastCategory != kIgnoreCategory) {
      currentOffset = javaNextOffset(input, currentOffset);
    }
  }
  return currentOffset;
}

struct ForwardMatch {
  int32_t offset;
  int32_t acceptedEnd;
  uint16_t state;
};

ForwardMatch startForwardMatch(const icu::UnicodeString& input, int32_t start) {
  return {
      start,
      start < input.length() ? nextCodePointOffset(input, start) : start,
      kStartState};
}

FOLLY_ALWAYS_INLINE void advanceForwardMatch(
    ForwardMatch& match,
    UChar32 codePoint) {
  const auto nextOffset = match.offset + U16_LENGTH(codePoint);
  const auto category = lookupJavaWordCategory(codePoint);
  if (category != kIgnoreCategory) {
    match.state = lookupForwardState(match.state, category);
  }
  if (kJavaWordEndStates[match.state]) {
    match.acceptedEnd = nextOffset;
  }
  match.offset = nextOffset;
}

int32_t findJavaNextBoundary(const icu::UnicodeString& input, int32_t start) {
  if (start >= input.length()) {
    return kBreakIteratorDone;
  }

  ForwardMatch match{start, javaNextOffset(input, start), kStartState};
  while (match.offset < input.length() && match.state != kStopState) {
    const auto codePoint = javaCurrent(input, match.offset);
    if (codePoint == kCharacterIteratorDone) {
      break;
    }
    advanceForwardMatch(match, codePoint);
  }
  return match.acceptedEnd;
}

int32_t javaFollowing(const icu::UnicodeString& input, int32_t offset) {
  auto boundary = findJavaSafeBoundary(input, offset);
  while (boundary != kBreakIteratorDone && boundary <= offset) {
    boundary = findJavaNextBoundary(input, boundary);
  }
  return boundary;
}

FOLLY_ALWAYS_INLINE bool isJavaWordBoundary(
    const icu::UnicodeString& input,
    int32_t offset) {
  return offset == 0 || javaFollowing(input, offset - 1) == offset;
}

/// Directly follows ConditionalSpecialCasing.isFinalCased() for strings whose
/// UTF-16 representation contains a surrogate pair.
bool isJavaFinalSigmaWithSurrogates(
    const icu::UnicodeString& input,
    int32_t sigmaOffset) {
  UChar32 codePoint = 0;
  for (auto offset = sigmaOffset;
       offset >= 0 && !isJavaWordBoundary(input, offset);
       offset -= U16_LENGTH(codePoint)) {
    codePoint = input.char32At(offset - 1);
    if (!isJavaCased(codePoint)) {
      continue;
    }

    for (auto followingOffset = sigmaOffset + 1;
         followingOffset < input.length() &&
         !isJavaWordBoundary(input, followingOffset);
         followingOffset += U16_LENGTH(codePoint)) {
      codePoint = input.char32At(followingOffset);
      if (isJavaCased(codePoint)) {
        return false;
      }
    }
    return true;
  }
  return false;
}

/// Implements the word-boundary part of OpenJDK
/// ConditionalSpecialCasing.isFinalCased(). An accepting DFA position beyond a
/// cased character proves that it belongs to the same longest-match word,
/// allowing early exit without first scanning to the end of the word.
enum class FinalSigmaResult : uint8_t {
  kNotFinal,
  kFinal,
  kNeedsUtf16Fallback,
};

FinalSigmaResult evaluateJavaFinalSigma(
    const icu::UnicodeString& input,
    int32_t sigmaOffset) {
  const auto safeBoundary = findSafeBoundaryBefore(input, sigmaOffset);
  if (safeBoundary.needsUtf16Fallback) {
    return FinalSigmaResult::kNeedsUtf16Fallback;
  }
  auto boundary = safeBoundary.offset;

  while (boundary < input.length()) {
    auto match = startForwardMatch(input, boundary);
    bool hasCasedBeforeSigma = false;
    int32_t firstCasedAfterSigma = -1;

    while (match.offset < input.length() && match.state != kStopState) {
      const auto codePointOffset = match.offset;
      const auto codePoint = input.char32At(codePointOffset);
      if (codePoint == kCharacterIteratorDone) {
        break;
      }
      if (U16_LENGTH(codePoint) == 2) {
        return FinalSigmaResult::kNeedsUtf16Fallback;
      }

      if (codePointOffset < sigmaOffset) {
        if (!hasCasedBeforeSigma && isJavaCased(codePoint)) {
          hasCasedBeforeSigma = true;
        }
      } else if (
          codePointOffset > sigmaOffset && firstCasedAfterSigma < 0 &&
          isJavaCased(codePoint)) {
        firstCasedAfterSigma = codePointOffset;
      }

      advanceForwardMatch(match, codePoint);

      if (match.acceptedEnd > sigmaOffset) {
        if (!hasCasedBeforeSigma) {
          return FinalSigmaResult::kNotFinal;
        }
        if (firstCasedAfterSigma >= 0 &&
            match.acceptedEnd > firstCasedAfterSigma) {
          return FinalSigmaResult::kNotFinal;
        }
      }
    }

    if (match.acceptedEnd > sigmaOffset) {
      return hasCasedBeforeSigma ? FinalSigmaResult::kFinal
                                 : FinalSigmaResult::kNotFinal;
    }

    // The safe boundary can precede several complete words. Advance by the
    // exact boundary produced by the finished forward match and try again.
    boundary = match.acceptedEnd;
  }
  return FinalSigmaResult::kNotFinal;
}

} // namespace

void adjustJavaSigmaInPlace(
    icu::UnicodeString& input,
    folly::Range<const int32_t*> sigmaOffsets) {
  for (const auto offset : sigmaOffsets) {
    const auto evaluation = evaluateJavaFinalSigma(input, offset);
    const auto isFinal = evaluation == FinalSigmaResult::kNeedsUtf16Fallback
        ? isJavaFinalSigmaWithSurrogates(input, offset)
        : evaluation == FinalSigmaResult::kFinal;
    input.setCharAt(offset, isFinal ? kFinalSigma : kSmallSigma);
  }
}

} // namespace bytedance::bolt::functions::stringCore::spark
