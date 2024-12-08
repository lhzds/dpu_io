#include "config.h"
#include "docacxx/ctx/consumer.h"
#include "docacxx/ctx/server.h"
#include "doca_wrapper/specialized_producer.h"
#include <doca_pe.h>
#include <optional>

using namespace docacxx;
using namespace pulsedb;

int main() {
    doca_dev *dev{};
    doca_dev_rep *dev_rep{};
    doca_pe *pe{};
    check_derr(open_doca_device_with_pci(DPU_DEV_PCI, &dev),
        "failed to open dev");
    check_derr(open_doca_device_rep_with_pci(dev, DOCA_DEVINFO_REP_FILTER_NET,
        DPU_DEV_REP_PCI, &dev_rep), "failed to open dev rep");
    check_derr(doca_pe_create(&pe), "failed to create pe");

    comch::Server server(SERVER_NAME, dev, dev_rep, pe, comch::Server::Cfg{0});
    doca_comch_connection *conn{};
    server.set_new_conn_cb([&](doca_comch_connection *c) {
        conn = c;
    });
    std::optional<size_t> remote_consumer_id{};
    server.set_new_consumer_cb([&](doca_comch_connection *conn, uint32_t id) {
        remote_consumer_id = id;
    });
    server.start_sync();

    // 创建 producer
    while (not remote_consumer_id) {
        doca_pe_progress(pe);
    }
    SpecializedProducer producer(conn, pe, dev, SpecializedProducer::Cfg{1}, *remote_consumer_id);
    producer.start_sync();

    // 创建 consumer
    Mmap dummy_consumer_mmap(dev, 1);
    comch::Consumer consumer(conn, pe, pe, dummy_consumer_mmap, dev, comch::Consumer::Cfg{1});
    consumer.start_sync();

    // 接受消息
    bool is_recved{};
    auto task = consumer.alloc_post_recv_task([&](const auto &task) {
        is_recved = true;
        auto *imm_data = task.imm_data();
        auto imm_data_size = task.imm_data_size();
        std::cout << std::string_view(imm_data, imm_data_size) << std::endl;
    });
    task = consumer.submit(std::move(task));
    assert(not task);
    while (not is_recved) {
        doca_pe_progress(pe);
    }

    if (auto result = doca_pe_destroy(pe); result != DOCA_SUCCESS) {
        std::cerr << "failed to destroy pe: " << doca_error_get_descr(result) << std::endl;
    }
    if (auto result = doca_dev_rep_close(dev_rep); result != DOCA_SUCCESS) {
        std::cerr << "failed to close dev rep: " << doca_error_get_descr(result) << std::endl;
    }
    if (auto result = doca_dev_close(dev); result != DOCA_SUCCESS) {
        std::cerr << "failed to close dev: " << doca_error_get_descr(result) << std::endl;
    }
    return 0;
}
