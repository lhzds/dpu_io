#include "bytes.h"
#include "docacxx/ctx/client.h"
#include "docacxx/ctx/dma.h"
#include "docacxx/mem.h"
#include "scoped_guard.h"
#include "util.h"

#include <argparse/argparse.hpp>
#include <doca_pe.h>
#include <nlohmann/json.hpp>
#include <string>

using namespace pulsedb;

struct InitMsgHeader {
    char *remote_mem_addr;
    size_t remote_mem_size;
    size_t remote_mmap_desc_size;
};

struct Options {
    std::string server_name;
    std::string dev_pci;
    size_t dma_buf_max_size{};
};

Options read_args(int argc, char **argv) {
    argparse::ArgumentParser program("comch_lat_host");

    program.add_argument("--server_name")
        .help("comch server name")
        .required();
    program.add_argument("--dev_pci")
        .help("host dev pci")
        .required();
    program.add_argument("--dma_buf_max_size")
        .help("dma buffer max size")
        .required()
        .scan<'d', size_t>();

    try {
        program.parse_args(argc, argv);
    }
    catch (const std::exception &err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        std::exit(1);
    }

    Options opts;
    opts.server_name = program.get<std::string>("--server_name");
    opts.dev_pci = program.get<std::string>("--dev_pci");
    opts.dma_buf_max_size = program.get<size_t>("--dma_buf_max_size");
    return opts;
}

int main(int argc, char **argv) {
    auto opts = read_args(argc, argv);

    doca_dev *dev{};
    doca_pe *pe{};
    docacxx::check_derr(docacxx::open_doca_device_with_pci(opts.dev_pci.c_str(), &dev),
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
    docacxx::comch::Client client(opts.server_name.c_str(), dev, pe, client_cfg);

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
    docacxx::Mmap local_mmap(dev, opts.dma_buf_max_size);
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
