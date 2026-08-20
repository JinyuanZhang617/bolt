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

#include <algorithm>
#include <cstdint>
#include <limits>

#include <folly/CPortability.h>
#include <folly/small_vector.h>
#include <unicode/uchar.h>
#include <unicode/unistr.h>

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

enum JavaWordProperty : uint32_t {
  kJavaWordNone = 0,
  kJavaWordIgnore = 1U << 0,
  kJavaWordEnclosing = 1U << 1,
  kJavaWordDanda = 1U << 2,
  kJavaWordKanji = 1U << 3,
  kJavaWordKatakana = 1U << 4,
  kJavaWordHiragana = 1U << 5,
  kJavaWordCjkDiacritic = 1U << 6,
  kJavaWordLetter = 1U << 7,
  kJavaWordDigit = 1U << 8,
  kJavaWordMidWord = 1U << 9,
  kJavaWordMidNumber = 1U << 10,
  kJavaWordPreNumber = 1U << 11,
  kJavaWordPostNumber = 1U << 12,
  kJavaWordWhitespace = 1U << 13,
  kJavaWordLineSeparator = 1U << 14,
  kJavaWordBase = 1U << 15,
};

FOLLY_ALWAYS_INLINE bool isJavaKanji(UChar32 codePoint) {
  return codePoint == 0x3005 || (codePoint >= 0x4E00 && codePoint <= 0x9FA5) ||
      (codePoint >= 0xF900 && codePoint <= 0xFA2D);
}

FOLLY_ALWAYS_INLINE uint32_t javaWordProperties(UChar32 codePoint) {
  const auto category = u_charType(codePoint);
  uint32_t properties = kJavaWordNone;

  if (category == U_FORMAT_CHAR) {
    properties |= kJavaWordIgnore;
  }
  if (category == U_NON_SPACING_MARK || category == U_ENCLOSING_MARK) {
    properties |= kJavaWordEnclosing;
  }
  if (codePoint == 0x0964 || codePoint == 0x0965) {
    properties |= kJavaWordDanda;
  }

  const bool kanji = isJavaKanji(codePoint);
  const bool katakana = (codePoint >= 0x30A1 && codePoint <= 0x30FA) ||
      codePoint == 0x30FD || codePoint == 0x30FE;
  const bool hiragana = (codePoint >= 0x3041 && codePoint <= 0x3094) ||
      codePoint == 0x309D || codePoint == 0x309E;
  const bool cjkDiacritic = (codePoint >= 0x3099 && codePoint <= 0x309C) ||
      codePoint == 0x30FB || codePoint == 0x30FC;

  if (kanji) {
    properties |= kJavaWordKanji;
  }
  if (katakana) {
    properties |= kJavaWordKatakana;
  }
  if (hiragana) {
    properties |= kJavaWordHiragana;
  }
  if (cjkDiacritic) {
    properties |= kJavaWordCjkDiacritic;
  }

  const bool isLetterCategory = category == U_UPPERCASE_LETTER ||
      category == U_LOWERCASE_LETTER || category == U_TITLECASE_LETTER ||
      category == U_MODIFIER_LETTER || category == U_OTHER_LETTER;
  if ((isLetterCategory || category == U_COMBINING_SPACING_MARK) && !kanji &&
      !katakana && !hiragana && !cjkDiacritic) {
    properties |= kJavaWordLetter;
  }
  if (category == U_DECIMAL_DIGIT_NUMBER || category == U_LETTER_NUMBER ||
      category == U_OTHER_NUMBER) {
    properties |= kJavaWordDigit;
  }
  if (category == U_DASH_PUNCTUATION || category == U_CONNECTOR_PUNCTUATION ||
      codePoint == 0x00AD || codePoint == 0x2027 || codePoint == '"' ||
      codePoint == '\'' || codePoint == '.') {
    properties |= kJavaWordMidWord;
  }
  if (codePoint == '"' || codePoint == '\'' || codePoint == ',' ||
      codePoint == 0x066B || codePoint == '.') {
    properties |= kJavaWordMidNumber;
  }
  if ((category == U_CURRENCY_SYMBOL && codePoint != 0x00A2) ||
      codePoint == '#' || codePoint == '.') {
    properties |= kJavaWordPreNumber;
  }
  if (codePoint == '%' || codePoint == '&' || codePoint == 0x00A2 ||
      codePoint == 0x066A || codePoint == 0x2030 || codePoint == 0x2031) {
    properties |= kJavaWordPostNumber;
  }
  if (category == U_SPACE_SEPARATOR || codePoint == '\t') {
    properties |= kJavaWordWhitespace;
  }
  if (codePoint == '\n' || codePoint == '\f' || codePoint == 0x2028 ||
      codePoint == 0x2029) {
    properties |= kJavaWordLineSeparator;
  }
  if (category != U_NON_SPACING_MARK && category != U_ENCLOSING_MARK &&
      category != U_CONTROL_CHAR && category != U_FORMAT_CHAR &&
      category != U_LINE_SEPARATOR && category != U_PARAGRAPH_SEPARATOR) {
    properties |= kJavaWordBase;
  }

  return properties;
}

class JavaWordCursor {
 public:
  JavaWordCursor(const icu::UnicodeString& input, int32_t offset)
      : input_(&input), offset_(offset) {
    skipIgnored();
  }

  bool atEnd() const {
    return offset_ >= input_->length();
  }

  int32_t offset() const {
    return offset_;
  }

  UChar32 codePoint() const {
    return input_->char32At(offset_);
  }

  uint32_t properties() const {
    return atEnd() ? kJavaWordNone : javaWordProperties(codePoint());
  }

  bool consume(uint32_t property) {
    if ((properties() & property) == 0) {
      return false;
    }
    consumeCurrent();
    return true;
  }

  void consumeCurrent() {
    if (!atEnd()) {
      offset_ += U16_LENGTH(codePoint());
      skipIgnored();
    }
  }

 private:
  void skipIgnored() {
    while (offset_ < input_->length()) {
      const auto current = input_->char32At(offset_);
      if ((javaWordProperties(current) & kJavaWordIgnore) == 0) {
        break;
      }
      offset_ += U16_LENGTH(current);
    }
  }

  const icu::UnicodeString* input_;
  int32_t offset_;
};

FOLLY_ALWAYS_INLINE bool consumeJavaLetter(JavaWordCursor& cursor) {
  auto trial = cursor;
  if (!trial.consume(kJavaWordLetter)) {
    return false;
  }
  while (trial.consume(kJavaWordEnclosing)) {
  }
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE bool consumeJavaDigit(JavaWordCursor& cursor) {
  auto trial = cursor;
  if (!trial.consume(kJavaWordDigit)) {
    return false;
  }
  while (trial.consume(kJavaWordEnclosing)) {
  }
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE bool consumeJavaWord(JavaWordCursor& cursor) {
  auto trial = cursor;
  if (!consumeJavaLetter(trial)) {
    return false;
  }
  while (consumeJavaLetter(trial)) {
  }
  while (true) {
    auto continuation = trial;
    if (!continuation.consume(kJavaWordMidWord) ||
        !consumeJavaLetter(continuation)) {
      break;
    }
    while (consumeJavaLetter(continuation)) {
    }
    trial = continuation;
  }
  trial.consume(kJavaWordDanda);
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE bool consumeJavaNumber(JavaWordCursor& cursor) {
  auto trial = cursor;
  if (!consumeJavaDigit(trial)) {
    return false;
  }
  while (consumeJavaDigit(trial)) {
  }
  while (true) {
    auto continuation = trial;
    if (!continuation.consume(kJavaWordMidNumber) ||
        !consumeJavaDigit(continuation)) {
      break;
    }
    while (consumeJavaDigit(continuation)) {
    }
    trial = continuation;
  }
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE bool consumeJavaWordNumberSequence(JavaWordCursor& cursor) {
  auto trial = cursor;
  bool lastWasNumber = false;
  if (consumeJavaWord(trial)) {
    lastWasNumber = false;
  } else if (consumeJavaNumber(trial)) {
    lastWasNumber = true;
  } else {
    return false;
  }

  while (true) {
    auto continuation = trial;
    const bool consumed = lastWasNumber ? consumeJavaWord(continuation)
                                        : consumeJavaNumber(continuation);
    if (!consumed) {
      break;
    }
    lastWasNumber = !lastWasNumber;
    trial = continuation;
  }
  if (lastWasNumber) {
    trial.consume(kJavaWordPostNumber);
  }
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE bool consumeJavaPrefixedNumberSequence(
    JavaWordCursor& cursor) {
  auto trial = cursor;
  if (!trial.consume(kJavaWordPreNumber) || !consumeJavaNumber(trial)) {
    return false;
  }

  bool lastWasNumber = true;
  while (true) {
    auto continuation = trial;
    const bool consumed = lastWasNumber ? consumeJavaWord(continuation)
                                        : consumeJavaNumber(continuation);
    if (!consumed) {
      break;
    }
    lastWasNumber = !lastWasNumber;
    trial = continuation;
  }
  if (lastWasNumber) {
    trial.consume(kJavaWordPostNumber);
  }
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE bool consumeJavaWhitespace(JavaWordCursor& cursor) {
  auto trial = cursor;
  bool consumed = false;
  while (trial.consume(kJavaWordWhitespace)) {
    consumed = true;
  }
  if (!trial.atEnd() && trial.codePoint() == '\r') {
    trial.consumeCurrent();
    consumed = true;
  }
  if (trial.consume(kJavaWordLineSeparator)) {
    consumed = true;
  }
  if (!consumed) {
    return false;
  }
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE bool consumeJavaRun(
    JavaWordCursor& cursor,
    uint32_t properties) {
  auto trial = cursor;
  if (!trial.consume(properties)) {
    return false;
  }
  while (trial.consume(properties)) {
  }
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE bool consumeJavaBaseWithEnclosingMarks(
    JavaWordCursor& cursor) {
  auto trial = cursor;
  if (!trial.consume(kJavaWordBase) || !trial.consume(kJavaWordEnclosing)) {
    return false;
  }
  while (trial.consume(kJavaWordEnclosing)) {
  }
  cursor = trial;
  return true;
}

FOLLY_ALWAYS_INLINE int32_t
nextJavaWordBoundary(const icu::UnicodeString& input, int32_t start) {
  JavaWordCursor fallback(input, start);
  if (fallback.atEnd()) {
    return input.length();
  }
  fallback.consumeCurrent();
  auto end = fallback.offset();

  const auto consider = [&](auto consume) {
    JavaWordCursor candidate(input, start);
    if (consume(candidate)) {
      end = std::max(end, candidate.offset());
    }
  };

  consider(consumeJavaWordNumberSequence);
  consider(consumeJavaPrefixedNumberSequence);
  consider(consumeJavaWhitespace);
  consider([](JavaWordCursor& cursor) {
    return consumeJavaRun(cursor, kJavaWordKatakana | kJavaWordCjkDiacritic);
  });
  consider([](JavaWordCursor& cursor) {
    return consumeJavaRun(cursor, kJavaWordHiragana | kJavaWordCjkDiacritic);
  });
  consider([](JavaWordCursor& cursor) {
    return consumeJavaRun(cursor, kJavaWordKanji);
  });
  consider(consumeJavaBaseWithEnclosingMarks);
  return end;
}

FOLLY_ALWAYS_INLINE void adjustJavaSigmaInPlace(
    icu::UnicodeString& input,
    int32_t sigmaOffset) {
  struct Replacement {
    int32_t offset;
    char16_t codePoint;
  };

  if (sigmaOffset < 0) {
    return;
  }

  folly::small_vector<Replacement, 4> replacements;
  auto segmentStart = int32_t{0};
  while (segmentStart < input.length()) {
    const auto segmentEnd = nextJavaWordBoundary(input, segmentStart);
    auto casedCount = uint32_t{0};
    auto lastCasedOffset = int32_t{-1};
    auto lastSigmaReplacement = std::numeric_limits<size_t>::max();

    auto offset = segmentStart;
    while (offset < segmentEnd) {
      const auto codePoint = input.char32At(offset);
      if (codePoint == 0x03A3) {
        replacements.push_back({offset, char16_t{0x03C3}});
        lastSigmaReplacement = replacements.size() - 1;
      }
      if (isJavaCased(codePoint)) {
        ++casedCount;
        lastCasedOffset = offset;
      }
      offset += U16_LENGTH(codePoint);
    }

    if (casedCount >= 2 &&
        lastSigmaReplacement != std::numeric_limits<size_t>::max() &&
        replacements[lastSigmaReplacement].offset == lastCasedOffset) {
      replacements[lastSigmaReplacement].codePoint = char16_t{0x03C2};
    }
    segmentStart = segmentEnd;
  }

  for (const auto& replacement : replacements) {
    input.setCharAt(replacement.offset, replacement.codePoint);
  }
}

} // namespace bytedance::bolt::functions::stringCore::spark
