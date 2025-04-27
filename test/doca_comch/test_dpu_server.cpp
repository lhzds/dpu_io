#include "config.h"
#include "docacxx/ctx/server.h"
#include "doca_wrapper/producers.h"
#include "scoped_guard.h"
#include <doca_pe.h>
#include <optional>

using namespace docacxx;
using namespace pulsedb;

int main() {
    doca_dev *dev{};
    doca_dev_rep *dev_rep{};
    doca_pe *pe{};
    check_derr(open_doca_device_with_pci(kDpuDevPci, &dev),
        "failed to open dev");
    check_derr(open_doca_device_rep_with_pci(dev, DOCA_DEVINFO_REP_FILTER_NET,
        kDpuDevRepPci, &dev_rep), "failed to open dev rep");
    check_derr(doca_pe_create(&pe), "failed to create pe");
    
    ScopeGuard guard([&] {
        if (auto result = doca_pe_destroy(pe); result != DOCA_SUCCESS) {
            std::cerr << "failed to destroy pe: " << doca_error_get_descr(result) << std::endl;
        }
        if (auto result = doca_dev_rep_close(dev_rep); result != DOCA_SUCCESS) {
            std::cerr << "failed to close dev rep: " << doca_error_get_descr(result) << std::endl;
        }
        if (auto result = doca_dev_close(dev); result != DOCA_SUCCESS) {
            std::cerr << "failed to close dev: " << doca_error_get_descr(result) << std::endl;
        }
    });

    comch::Server::Cfg server_cfg;
    server_cfg.max_send_task_slot_num = 1;
    comch::Server server(kServerName, dev, dev_rep, pe, server_cfg);
    doca_comch_connection *conn{};
    server.set_new_conn_cb([&](doca_comch_connection *c) {
        conn = c;
    });
    std::optional<size_t > remote_consumer_id{};
    server.set_new_consumer_cb([&](doca_comch_connection *conn, uint32_t id) {
        remote_consumer_id = id;
    });
    server.start_sync();

    // 创建 producer
    while (not remote_consumer_id) {
        doca_pe_progress(pe);
    }
    SyncSingleTaskProducer producer(conn, pe, dev, 1024, *remote_consumer_id);
    producer.start_sync();

    std::string msg = "hello, world!";
    std::memcpy(producer.buf_data(), msg.data(), msg.size());
    producer.send_sync(msg.size());

    producer.clean_sync();
    server.clean_sync();
    return 0;
}
