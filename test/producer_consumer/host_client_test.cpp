#include "config.h"
#include "docacxx/ctx/client.h"
#include "docacxx/ctx/consumer.h"
#include "doca_wrapper/specialized_producer.h"
#include <doca_pe.h>
#include <optional>

using namespace docacxx;
using namespace pulsedb;

int main() {
    doca_dev *dev{};
    doca_pe *pe{};
    check_derr(open_doca_device_with_pci(HOST_DEV_PCI, &dev),
        "failed to open dev");
    check_derr(doca_pe_create(&pe), "failed to create pe");

    comch::Client client(SERVER_NAME, dev, pe, comch::Client::Cfg{0});
    std::optional<size_t> remote_consumer_id{};
    client.set_new_consumer_cb([&](doca_comch_connection *,uint32_t id) {
        remote_consumer_id = id;
    });
    client.start_sync();
    doca_comch_connection *conn = client.get_connection();

    // 创建 consumer
    Mmap dummy_consumer_mmap(dev, 1);
    comch::Consumer consumer(conn, pe, pe, dummy_consumer_mmap, dev, comch::Consumer::Cfg{1});
    consumer.start_sync();

    // 创建 producer
    while (not remote_consumer_id) {
        doca_pe_progress(pe);
    }
    SpecializedProducer producer(conn, pe, dev, SpecializedProducer::Cfg{1}, *remote_consumer_id);
    producer.start_sync();

    // 发送消息
    std::string msg("hello, dpu!");
    bool is_sended{};
    auto task = producer.alloc_send_task((uint8_t *)msg.data(), msg.size(), [&](const auto &task) {
        is_sended = true;
    });
    task = producer.submit(std::move(task));
    while (task) {
        doca_pe_progress(pe);
        task = producer.submit(std::move(task));
    }
    while (not is_sended) {
        doca_pe_progress(pe);
    }

    if (auto result = doca_pe_destroy(pe); result != DOCA_SUCCESS) {
        std::cerr << "failed to destroy pe: " << doca_error_get_descr(result) << std::endl;
    }
    if (auto result = doca_dev_close(dev); result != DOCA_SUCCESS) {
        std::cerr << "failed to close dev: " << doca_error_get_descr(result) << std::endl;
    }
    return 0;
}
