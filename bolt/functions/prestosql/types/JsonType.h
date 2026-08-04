/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "folly/CPortability.h"
#include "folly/Conv.h"
#include "folly/json.h"

#include "bolt/expression/CastExpr.h"
#include "bolt/expression/VectorWriters.h"
#include "bolt/functions/prestosql/json/SIMDJsonUtil.h"
#include "bolt/type/Type.h"
#include "bolt/vector/VectorTypeUtils.h"
namespace bytedance::bolt {

/// Custom operator for casts from and to Json type.
class JsonCastOperator : public exec::CastOperator {
 public:
  static const std::shared_ptr<const CastOperator>& get() {
    // mark `thread_local` keyword to avoid data race competition caused by
    // multiple threads simultaneously manipulating member variables in static
    thread_local const std::shared_ptr<const CastOperator> instance{
        new JsonCastOperator()};
    return instance;
  }

  bool isSupportedFromType(const TypePtr& other) const override;

  bool isSupportedToType(const TypePtr& other) const override;

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override;

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result,
      const bool isToJson,
      const bool isTopLevel,
      const bool sparkTimestampMapKey = false) const;

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override;

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result,
      const bool isFromJson = false,
      const VectorPtr& logFailuresOnly = nullptr) const;

  JsonCastOperator() = default;

 private:
  template <TypeKind kind>
  void castFromJson(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result,
      const bool isFromJson,
      const VectorPtr& logFailuresOnly = nullptr) const;

  mutable folly::once_flag initializeErrors_;
  mutable std::exception_ptr errors_[simdjson::NUM_ERROR_CODES];
  mutable std::string paddedInput_;
};

/// Represents JSON as a string.
class JsonType : public VarcharType {
  JsonType() = default;

 public:
  static const std::shared_ptr<const JsonType>& get() {
    static const std::shared_ptr<const JsonType> instance{new JsonType()};

    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "JSON";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }
};

FOLLY_ALWAYS_INLINE bool isJsonType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return JsonType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const JsonType> JSON() {
  return JsonType::get();
}

// Type used for function registration.
struct JsonT {
  using type = Varchar;
  static constexpr const char* typeName = "json";
};

using Json = CustomType<JsonT>;

void registerJsonType();

} // namespace bytedance::bolt
