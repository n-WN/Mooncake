// TSAN repro for issue #2477 against the PRE-FIX code (in-place mutation of
// the local SegmentDesc). Mirrors ConcurrentWritersVsSnapshotReaders from the
// fix branch, ported to the old SegmentTracker(SegmentDescRef) API.
// Expected: ThreadSanitizer data race reports on
// MemorySegmentDesc::buffers (push_back/erase/sort vs. reader iteration),
// and/or invariant failures from torn reads.

#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_tracker.h"

namespace mooncake {
namespace tent {

namespace {

std::string locationFor(uint64_t addr) {
    return "cpu:" + std::to_string(addr % 4096);
}

BufferDesc makeBuffer(uint64_t addr, uint64_t length) {
    BufferDesc desc;
    desc.addr = addr;
    desc.length = length;
    desc.location = locationFor(addr);
    desc.ref_count = 1;
    return desc;
}

const std::vector<BufferDesc>& buffersOf(const SegmentDescRef& snapshot) {
    return std::get<MemorySegmentDesc>(snapshot->detail).buffers;
}

}  // namespace

TEST(SegmentTrackerRaceRepro, ConcurrentWritersVsReaders) {
    auto manager = std::make_unique<SegmentManager>(nullptr);
    {
        auto desc = manager->getLocal();
        desc->name = "local_test_segment";
        desc->type = SegmentType::Memory;
        desc->machine_id = "test_machine";
    }
    SegmentTracker tracker(manager->getLocal());

    constexpr int kWriters = 4;
    constexpr int kReaders = 4;
    constexpr int kIterations = 300;
    constexpr int kBuffersPerBatch = 8;
    constexpr uint64_t kLength = 0x1000;

    std::atomic<bool> done{false};
    std::atomic<int> failures{0};

    auto noop = [](std::vector<BufferDesc>&) -> Status { return Status::OK(); };

    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            const uint64_t base = (w + 1) * 0x100000000ULL;
            for (int iter = 0; iter < kIterations; ++iter) {
                std::vector<BufferDesc> batch;
                for (int i = 0; i < kBuffersPerBatch; ++i) {
                    batch.push_back(
                        makeBuffer(base + i * kLength * 2, kLength));
                }
                if (!tracker.addInBatch(batch, noop).ok()) failures++;
                for (int i = 0; i < kBuffersPerBatch; ++i) {
                    auto on_remove = [](BufferDesc&) -> Status {
                        return Status::OK();
                    };
                    if (!tracker
                             .remove(base + i * kLength * 2, kLength, on_remove)
                             .ok())
                        failures++;
                }
            }
        });
    }

    std::vector<std::thread> readers;
    for (int r = 0; r < kReaders; ++r) {
        readers.emplace_back([&] {
            uint64_t rounds = 0;
            while (!done.load(std::memory_order_acquire)) {
                auto snapshot = manager->getLocal();
                const auto& buffers = buffersOf(snapshot);
                uint64_t prev_addr = 0;
                for (const auto& buf : buffers) {
                    if (buf.length != kLength ||
                        buf.location != locationFor(buf.addr) ||
                        buf.addr < prev_addr) {
                        failures++;
                    }
                    prev_addr = buf.addr;
                }
                snapshot->findBuffer(0x100000000ULL, kLength);
                if (rounds++ % 64 == 0) {
                    auto dump = manager->getLocalDumpedJson();
                    if (!dump || dump->empty()) failures++;
                }
            }
        });
    }

    for (auto& t : writers) t.join();
    done.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();

    EXPECT_EQ(failures.load(), 0);
}

}  // namespace tent
}  // namespace mooncake
