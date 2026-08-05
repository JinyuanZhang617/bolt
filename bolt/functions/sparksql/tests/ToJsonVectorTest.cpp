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

#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/sparksql/tests/JsonTestUtil.h"

using namespace bytedance::bolt::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class ToJsonVectorTest : public SparkFunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    SparkFunctionBaseTest::SetUpTestCase();
    sparksql::registerFunctions("");
  }

  void testToJson(const VectorPtr& input, const VectorPtr& expected) {
    assertEqualVectors(
        expected, evaluate("to_json(c0)", makeRowVector({input})));
  }

  void setTimezone(const std::string& value) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, value},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  template <typename T>
  RowVectorPtr makeRowWithDictionaryElements(
      const std::vector<std::vector<std::optional<T>>>& elements,
      const TypePtr& rowType) {
    std::vector<VectorPtr> children;
    children.reserve(elements.size());
    for (auto i = 0; i < elements.size(); ++i) {
      children.push_back(
          makeDictionaryVector(elements[i], rowType->childAt(i)));
    }
    return std::make_shared<RowVector>(
        pool(), rowType, nullptr, elements.front().size(), children);
  }
};

TEST_F(ToJsonVectorTest, registeredAsVectorFunction) {
  const std::vector<TypePtr> inputTypes = {MAP(TIMESTAMP(), VARCHAR())};
  EXPECT_EQ(exec::resolveVectorFunction("to_json", inputTypes), VARCHAR());

  core::QueryConfig config({});
  EXPECT_NE(
      exec::getVectorFunction("to_json", inputTypes, {}, config), nullptr);
}

TEST_F(ToJsonVectorTest, array) {
  auto input = makeNullableArrayVector<std::string>(
      {{{"red", "blue"}},
       {{std::nullopt, "purple"}},
       emptyArray,
       std::nullopt});
  auto expected = makeNullableFlatVector<std::string>(
      {R"(["red","blue"])", R"([null,"purple"])", R"([])", std::nullopt});
  testToJson(input, expected);

  input = makeArrayWithDictionaryElements<std::string>(
      {"red", "blue", "green", "purple", "orange"}, 2);
  expected = makeFlatVector<std::string>(
      {R"([null,"purple"])", R"(["green","blue"])", R"(["red"])"});
  testToJson(input, expected);
}

TEST_F(ToJsonVectorTest, map) {
  auto input = makeNullableMapVector<std::string, int64_t>(
      {{{{"blue", 1}, {"red", 2}}},
       {{{"purple", std::nullopt}, {"orange", -2}}},
       emptyArray,
       std::nullopt});
  auto expected = makeNullableFlatVector<std::string>(
      {R"({"blue":1,"red":2})",
       R"({"purple":null,"orange":-2})",
       R"({})",
       std::nullopt});
  testToJson(input, expected);

  std::vector<std::vector<std::pair<int16_t, std::optional<int64_t>>>>
      integerKeyMaps{{{3, std::nullopt}, {4, 2}}, {}};
  auto integerKeys = makeMapVector<int16_t, int64_t>(
      integerKeyMaps, MAP(SMALLINT(), BIGINT()));
  expected = makeFlatVector<std::string>({R"({"3":null,"4":2})", R"({})"});
  testToJson(integerKeys, expected);

  auto floatingPointKeys = makeMapVector<double, int64_t>(
      {{{{4.4, std::nullopt}, {3.3, 2}}}, {}}, MAP(DOUBLE(), BIGINT()));
  expected = makeFlatVector<std::string>({R"({"4.4":null,"3.3":2})", R"({})"});
  testToJson(floatingPointKeys, expected);

  auto booleanKeys = makeMapVector<bool, int64_t>(
      {{{{true, std::nullopt}, {false, 2}}}, {}}, MAP(BOOLEAN(), BIGINT()));
  expected =
      makeFlatVector<std::string>({R"({"true":null,"false":2})", R"({})"});
  testToJson(booleanKeys, expected);
}

TEST_F(ToJsonVectorTest, row) {
  auto input = makeRowVector(
      {"id", "name", "score"},
      {makeNullableFlatVector<int64_t>({1, 2, std::nullopt}),
       makeNullableFlatVector<std::string>({"red", std::nullopt, std::nullopt}),
       makeNullableFlatVector<double>({1.5, std::nullopt, std::nullopt})});
  auto expected = makeFlatVector<std::string>(
      {R"({"id":1,"name":"red","score":1.5})", R"({"id":2})", R"({})"});
  testToJson(input, expected);
}

TEST_F(ToJsonVectorTest, nested) {
  std::vector<std::vector<std::optional<int64_t>>> arrayData{
      {1, 2}, {std::nullopt, 4}, {}};
  auto arrays = makeNullableArrayVector<int64_t>(arrayData);
  std::vector<std::optional<
      std::vector<std::pair<std::string, std::optional<int64_t>>>>>
      mapData{{{{"a", 1}, {"b", 2}}}, {{{"c", std::nullopt}}}, emptyArray};
  auto maps = makeNullableMapVector<std::string, int64_t>(mapData);
  std::vector<VectorPtr> children{arrays, maps};
  auto input = makeRowVector({"values", "attributes"}, children);
  auto expected = makeFlatVector<std::string>(
      {R"({"values":[1,2],"attributes":{"a":1,"b":2}})",
       R"({"values":[null,4],"attributes":{"c":null}})",
       R"({"values":[],"attributes":{}})"});
  testToJson(input, expected);
}

TEST_F(ToJsonVectorTest, decimalAndBinary) {
  auto decimals = makeArrayVector<int64_t>(
      {{0, 100}, {123456, 1234567890}}, DECIMAL(10, 5));
  auto expected = makeFlatVector<std::string>(
      {R"([0.00000,0.00100])", R"([1.23456,12345.67890])"});
  testToJson(decimals, expected);

  auto binary = makeNullableArrayVector<StringView>(
      {{"key1"_sv, "value1"_sv}, {"key2"_sv, "value2"_sv}}, ARRAY(VARBINARY()));
  expected = makeFlatVector<std::string>(
      {R"(["a2V5MQ==","dmFsdWUx"])", R"(["a2V5Mg==","dmFsdWUy"])"});
  testToJson(binary, expected);

  auto longDecimals = makeArrayVector<int128_t>(
      {{0, 100}, {123456, 123456789112LL}}, DECIMAL(30, 10));
  expected = makeFlatVector<std::string>(
      {R"([0E-10,1.00E-8])", R"([0.0000123456,12.3456789112])"});
  testToJson(longDecimals, expected);
}

TEST_F(ToJsonVectorTest, dictionaryUnknownAndAllNull) {
  auto unknownArray = makeArrayWithDictionaryElements<UnknownValue>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      2,
      ARRAY(UNKNOWN()));
  testToJson(
      unknownArray,
      makeFlatVector<std::string>({"[null,null]", "[null,null]"}));

  auto dictionaryArray =
      makeArrayWithDictionaryElements<int64_t>({1, -2, 3, -4, 5, -6, 7}, 2);
  testToJson(
      dictionaryArray,
      makeFlatVector<std::string>({"[null,-6]", "[5,-4]", "[3,-2]", "[1]"}));

  std::vector<std::optional<std::string>> keys{
      "a", "b", "c", "d", "e", "f", "g"};
  std::vector<std::optional<double>> values{
      1.1e3, 2.2, 3.14, -4.4, std::nullopt, -6e-10, -7.7};
  auto dictionaryMap = makeMapWithDictionaryElements(keys, values, 2);
  testToJson(
      dictionaryMap,
      makeFlatVector<std::string>(
          {R"({"g":null,"f":-6.0E-10})",
           R"({"e":null,"d":-4.4})",
           R"({"c":3.14,"b":2.2})",
           R"({"a":1100.0})"}));

  auto rowType = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  auto dictionaryRow = makeRowWithDictionaryElements<int64_t>(
      {{1, 2, 3, 4, 5, 6, 7}, {10, 20, 30, 40, 50, 60, 70}}, rowType);
  testToJson(
      dictionaryRow,
      makeFlatVector<std::string>(
          {R"({})",
           R"({"a":6,"b":60})",
           R"({"a":5,"b":50})",
           R"({"a":4,"b":40})",
           R"({"a":3,"b":30})",
           R"({"a":2,"b":20})",
           R"({"a":1,"b":10})"}));

  testToJson(
      makeAllNullArrayVector(5, BIGINT()),
      makeAllNullFlatVector<std::string>(5));
  testToJson(
      makeAllNullMapVector(5, VARCHAR(), BIGINT()),
      makeAllNullFlatVector<std::string>(5));
}

TEST_F(ToJsonVectorTest, specialFloatingPointValues) {
  constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  constexpr double kInfinity = std::numeric_limits<double>::infinity();
  auto input = makeMapVector<double, double>(
      {{{{1, kNaN}, {2, kInfinity}}}, {{{kNaN, -kInfinity}}}},
      MAP(DOUBLE(), DOUBLE()));
  auto expected = makeFlatVector<std::string>(
      {R"({"1.0":"NaN","2.0":Infinity})", R"({"NaN":-Infinity})"});
  testToJson(input, expected);
}

TEST_F(ToJsonVectorTest, timestampMapKeyUsesEpochMicros) {
  auto epochUs = makeFlatVector<int64_t>(
      {1451606400000000, 1451606400123000, 1451606400123456});
  auto input = makeRowVector({epochUs});

  setTimezone("UTC");
  auto expected = makeFlatVector<std::string>(
      {R"({"1451606400000000":"v"})",
       R"({"1451606400123000":"v"})",
       R"({"1451606400123456":"v"})"});
  assertEqualVectors(
      expected, evaluate("to_json(map(timestamp_micros(c0), 'v'))", input));
}

TEST_F(ToJsonVectorTest, unsupportedScalarInput) {
  auto input = makeFlatVector<int64_t>({1, 2, 3});
  EXPECT_THROW(
      evaluate("to_json(c0)", makeRowVector({input})), BoltRuntimeError);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
