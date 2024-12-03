#include <cstdlib>
#include <cstring>
#include <doca_error.h>
#include <memory>

#include "doca_wrapper/queue_dma.h"
#include "docacxx/mem.h"
#include "fmt/format.h"
#include "gtest/gtest.h"

constexpr char DEV_PCI_ADDR[] = "cb:00.0";
using namespace pulsedb;

class QueueDmaTest: public testing::Test {
public:
    static void SetUpTestSuite() {}
    static void TearDownTestSuite() {}

    virtual void SetUp() override {
        docacxx::check_derr(docacxx::open_doca_device_with_pci(DEV_PCI_ADDR, &dev),
            "failed to open dev");
        docacxx::check_derr(doca_pe_create(&pe), "failed to create pe");
    }

    virtual void TearDown() override {
        dma->clean_sync();
        src_mmap.reset();
        dst_mmap.reset();
        if (auto result = doca_pe_destroy(pe); result != DOCA_SUCCESS) {
            std::cout << "failed to destroy pe" << std::endl;
        }
        if (auto result = doca_dev_close(dev); result != DOCA_SUCCESS) {
            std::cout << "failed to close dev" << std::endl;
        }
    }

    void create_mmap(size_t mem_size) {
        src_mmap = std::make_unique<docacxx::Mmap>(dev, mem_size);
        dst_mmap = std::make_unique<docacxx::Mmap>(dev, mem_size);
    }

    void create_dma(size_t dma_task_slot_num) {
        dma = std::make_unique<QueueDma>(pe, dev, dma_task_slot_num);
        dma->start_sync();
    }

    doca_dev *dev{};
    doca_pe *pe{};
    std::unique_ptr<docacxx::Mmap> src_mmap;
    std::unique_ptr<docacxx::Mmap> dst_mmap;
    std::unique_ptr<QueueDma> dma;
};

TEST_F(QueueDmaTest, PipelineMemcpy) {
    // 实际任务数大于任务槽数
    size_t mem_size = (1 << 12);
    size_t cp_size = (1 << 3);
    size_t total_task_num = mem_size / cp_size;

    size_t task_slot_num = 128;
    size_t pending_task_num{};

    create_dma(task_slot_num);
    create_mmap(mem_size);

    for (size_t i{}; i < total_task_num; ++i) {
        std::memcpy(src_mmap->data() + i * sizeof(size_t), &i, sizeof(size_t));
    }
    for (size_t i{}; i < total_task_num; ++i) {
        ++pending_task_num;
        dma->post_memcpy(*src_mmap, i * sizeof(size_t), *dst_mmap, i * sizeof(size_t), sizeof(size_t), [&] {
            --pending_task_num;
        });
    }
    std::cout << fmt::format("submit {} memcpy tasks", pending_task_num) << std::endl;

    while (pending_task_num > 0) {
        doca_pe_progress(pe);
    }
    
    ASSERT_EQ(std::memcmp(src_mmap->data(), dst_mmap->data(), src_mmap->size()), 0);
}

TEST_F(QueueDmaTest, PipelineMemcpyV) {
    size_t mem_size = (1 << 12);
    size_t cp_size = (1 << 3);
    size_t total_task_num = mem_size / cp_size;

    size_t task_slot_num = 128;
    size_t pending_task_num{};

    create_dma(task_slot_num);
    create_mmap(mem_size);

    std::vector<QueueDma::MemcpySeg> segs;
    for (size_t i{}; i < total_task_num; ++i) {
        std::memcpy(src_mmap->data() + i * sizeof(size_t), &i, sizeof(size_t));
        segs.emplace_back(i * sizeof(size_t), i * sizeof(size_t), sizeof(size_t));
    }

    bool is_finished{};
    dma->post_memcpyv(*src_mmap, *dst_mmap, segs, [&] {
        is_finished = true;
    });
    while (not is_finished) {
        doca_pe_progress(pe);
    }
    
    ASSERT_EQ(std::memcmp(src_mmap->data(), dst_mmap->data(), src_mmap->size()), 0);
}

// TEST_F(QueueDmaTest, PipelineMemcpyBlob) {
//     size_t mem_size = dma->cap().m_max_buf_size * 2;
//     size_t task_slot_num = 1;

//     create_dma(task_slot_num);
//     create_mmap(mem_size);

//     std::memset(src_mmap->data(), 66, src_mmap->size());

//     bool is_finished{};
//     dma->post_memcpy(*src_mmap, 0, *dst_mmap, 0, src_mmap->size(), [&] {
//         is_finished = true;
//     });
//     while (not is_finished) {
//         doca_pe_progress(pe);
//     }
    
//     ASSERT_EQ(std::memcmp(src_mmap->data(), dst_mmap->data(), src_mmap->size()), 0);
// }

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();   
}
