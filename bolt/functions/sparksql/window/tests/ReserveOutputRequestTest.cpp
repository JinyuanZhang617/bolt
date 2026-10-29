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
 */
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/lib/window/tests/WindowTestBase.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace bytedance::bolt::exec::test;

namespace bytedance::bolt::window::test {
namespace {
class ReserveOutputRequestTest : public WindowTestBase {
 protected:
  void testOutputReservation(bool strings) {
    filesystems::registerLocalFileSystem();
    window::prestosql::registerAllWindowFunctions();
    constexpr vector_size_t kBatchRows = 65536;
    constexpr vector_size_t kBatches = 8;
    constexpr vector_size_t kRows = kBatchRows * kBatches;
    VectorPtr argument = strings
        ? makeFlatVector<std::string>(
              kBatchRows,
              [](auto row) { return std::string(96, row % 2 ? 'b' : 'a'); },
              [](auto row) { return row % 7 == 0; })
        : VectorPtr(makeFlatVector<double>(
              kBatchRows,
              [](auto) { return 1.0; },
              [](auto row) { return row % 7 == 0; }));
    auto data = makeRowVector(
        {"x", "partition_key"},
        {argument,
         makeFlatVector<int64_t>(kBatchRows, [](auto) { return 1; })});
    const std::string frame =
        " over (partition by partition_key rows between "
        "unbounded preceding and unbounded following)";
    const std::vector<std::string> functions = strings
        ? std::vector<std::string>{"min(x)" + frame, "max(x)" + frame}
        : std::vector<std::string>{
              "sum(x)" + frame,
              "avg(x)" + frame,
              "min(x)" + frame,
              "max(x)" + frame};
    auto node =
        PlanBuilder().values({data}).streamingWindow(functions).planNode();
    auto root =
        memory::memoryManager()->addRootPool("reserve-output", 512LL << 20);
    auto query = core::QueryCtx::create(
        nullptr,
        core::QueryConfig(
            {{core::QueryConfig::kSpillEnabled, "true"},
             {core::QueryConfig::kWindowSpillEnabled, "true"},
             {core::QueryConfig::kPreferredOutputBatchRows, "4096"},
             {core::QueryConfig::kMaxOutputBatchRows, "4096"}}),
        std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
        nullptr,
        root);
    auto task = exec::Task::create(
        "reserve-output",
        core::PlanFragment(node),
        0,
        query,
        exec::Task::ExecutionMode::kSerial);
    auto dir = TempDirectoryPath::create();
    task->setSpillDirectory(dir->path);
    exec::DriverCtx ctx(task, 0, 0, exec::kUngroupedGroupId, 0);
    ctx.driver = nullptr;
    exec::Window window(
        0, &ctx, std::dynamic_pointer_cast<const core::WindowNode>(node));
    window.initialize();
    for (int i = 0; i < kBatches; ++i) {
      window.addInput(data);
    }
    window.noMoreInput();
    if (!strings) {
      EXPECT_GE(window.pool()->availableReservation(), int64_t(kRows) * 8 * 4);
    }
    // Output must fit the reservation made during input, without capacity
    // growth.
    const auto capacity = root->reservedBytes();
    static_cast<memory::MemoryPoolImpl*>(root.get())
        ->testingSetCapacity(capacity);
    vector_size_t count = 0;
    while (auto output = window.getOutput()) {
      ASSERT_EQ(root->capacity(), capacity);
      for (vector_size_t row = 0; row < output->size(); ++row) {
        for (size_t f = 0; f < functions.size(); ++f) {
          const auto& result = output->childAt(2 + f);
          ASSERT_FALSE(result->isNullAt(row));
          if (strings) {
            ASSERT_EQ(
                result->as<SimpleVector<StringView>>()->valueAt(row).str(),
                std::string(96, f == 0 ? 'a' : 'b'));
          } else {
            const double expected = f == 0
                ? double((kBatchRows - (kBatchRows + 6) / 7) * kBatches)
                : 1.0;
            ASSERT_EQ(
                result->as<SimpleVector<double>>()->valueAt(row), expected);
          }
        }
      }
      count += output->size();
    }
    EXPECT_EQ(count, kRows);
    EXPECT_EQ(window.stats().rlock()->spilledRows, 0);
    window.close();
  }
};

TEST_F(ReserveOutputRequestTest, numericArgumentsAndResults) {
  testOutputReservation(false);
}

TEST_F(ReserveOutputRequestTest, stringArgumentsAndResults) {
  testOutputReservation(true);
}
} // namespace
} // namespace bytedance::bolt::window::test
