#include "config.h"
#include "doca_wrapper/passive_consumer.h"
#include "docacxx/ctx/client.h"
#include "scoped_guard.h"
#include <doca_pe.h>

using namespace docacxx;
using namespace pulsedb;

int main() {
    doca_dev *dev{};
    doca_pe *pe{};
    check_derr(open_doca_device_with_pci(kHostDevPci, &dev),
        "failed to open dev");
    check_derr(doca_pe_create(&pe), "failed to create pe");

    ScopeGuard guard([&] {
        if (auto result = doca_pe_destroy(pe); result != DOCA_SUCCESS) {
            std::cerr << "failed to destroy pe: " << doca_error_get_descr(result) << std::endl;
        }
        if (auto result = doca_dev_close(dev); result != DOCA_SUCCESS) {
            std::cerr << "failed to close dev: " << doca_error_get_descr(result) << std::endl;
        }
    });

    comch::Client::Cfg client_cfg;
    client_cfg.max_send_task_slot_num = 1;
    comch::Client client(kServerName, dev, pe, client_cfg);
    client.start_sync();
    doca_comch_connection *conn = client.get_connection();

    // 创建 consumer
    PassiveConsumer::Cfg consumer_cfg;
    consumer_cfg.max_msg_size = 1024;
    consumer_cfg.post_recv_task_slot_num = 1;
    size_t is_recved{};
    PassiveConsumer consumer(conn, pe, pe, dev, consumer_cfg, [&](const ConstBytesView &raw_msg) {
        is_recved = true;
        std::cout << std::string_view(raw_msg) << std::endl;
    });

    while (not is_recved) {
        doca_pe_progress(pe);
    }

    consumer.clean_sync();
    client.clean_sync();
    return 0;
}
