/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "folly/CPortability.h"
#include "folly/Conv.h"
#include "folly/json.h"
#include <glog/logging.h>

#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/prestosql/types/JsonType.h"
namespace bytedance::bolt::functions {

/// The input parameter type and output result type of the to_json function
/// refer to the definition of Spark, and the specific type refers to this link:
/// https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.to_json.html

using bytedance::bolt::JsonCastOperator;
using bytedance::bolt::exec::VectorFunction;

class ToJsonFunction : public VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK(
        args.size() == 1,
        "The to_json function accepts and only accepts one parameter as an input parameter.");
    BOLT_CHECK(
        (args[0]->typeKind() == TypeKind::ARRAY ||
         args[0]->typeKind() == TypeKind::MAP ||
         args[0]->typeKind() == TypeKind::ROW),
        "According to spark documents, to_json only support map, array, struct type parameter");
    LOG(INFO) << "[BoltToJsonExecution][log by jy] implementation=prestosql_vector_to_json"
              << ", input_type=" << args[0]->type()->toString()
              << ", selected_rows=" << rows.countSelected();

    auto castFactory = std::dynamic_pointer_cast<const JsonCastOperator>(
        JsonCastOperator::get());
    castFactory->castTo(
        *args[0].get(),
        context,
        rows,
        outputType,
        result,
        /*isToJson=*/true,
        /*isTopLevel=*/true);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("any")
                .build()};
  }
};

/// @brief
/// @param
/// @param signatures
/// @param make_unique
BOLT_DECLARE_VECTOR_FUNCTION(
    udf_to_json,
    ToJsonFunction::signatures(),
    std::make_unique<ToJsonFunction>());

} // namespace bytedance::bolt::functions
