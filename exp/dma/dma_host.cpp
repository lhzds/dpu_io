#include "bytes.h"
#include "config.h"
#include "docacxx/ctx/dma.h"
#include "docacxx/ctx/client.h"
#include "docacxx/mem.h"
#include "scoped_guard.h"
#include "util.h"

#include <argparse/argparse.hpp>
#include <doca_pe.h>
#include <nlohmann/json.hpp>

using namespace pulsedb;

int main() {
    doca_dev *dev{};
    doca_pe *pe{};
    docacxx::check_derr(docacxx::open_doca_device_with_pci(kHostDevPci, &dev),
        "failed to open dev");
    docacxx::check_derr(doca_pe_create(&pe), "failed to create pe");
    
    ScopeGuard guard([&] {
        if (auto result = doca_pe_destroy(pe); result != DOCA_SUCCESS) {
            std::cerr << "failed to destroy pe: " << doca_error_get_descr(result) << std::endl;
        }
        if (auto result = doca_dev_close(dev); result != DOCA_SUCCESS) {
            std::cerr << "failed to close dev: " << doca_error_get_descr(result) << std::endl;
        }
    });

    docacxx::comch::Client::Cfg client_cfg;
    client_cfg.max_send_task_slot_num = 1;
    docacxx::comch::Client client(kServerName, dev, pe, client_cfg);

    bool is_exited{};
    client.set_recv_cb([&](doca_comch_connection *, uint8_t *data, uint32_t len) {
        is_exited = true;
        std::string_view msg((char *)data, len);
        if (msg != "Exit") {
            throw_with("Unexpected msg");
        }
    });
    client.start_sync();
    auto *conn = client.get_connection();

    BytesBuffer buffer;
    docacxx::Mmap local_mmap(dev, DMA_BUF_MAX_SIZE);
    InitMsgHeader header;
    header.remote_mem_addr = local_mmap.data();
    header.remote_mem_size = local_mmap.size();
    auto export_desc = local_mmap.export_pci(dev);
    header.remote_mmap_desc_size = export_desc.size();
    buffer.append(ConstBytesView((char *)&header, sizeof(header)));
    buffer.append(export_desc);
    
    auto send_task = client.alloc_send_task(conn, buffer.data(), buffer.size(), nullptr);
    client.submit(std::move(send_task));

    while (not is_exited) {
        doca_pe_progress(pe);
    }
    client.clean_sync();
    return 0;
}
