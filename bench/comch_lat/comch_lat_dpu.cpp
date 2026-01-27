#include "docacxx/ctx/dma.h"
#include "docacxx/ctx/server.h"
#include "docacxx/mem.h"
#include "scoped_guard.h"
#include "test_util/clock.h"
#include "util.h"

#include <argparse/argparse.hpp>
#include <doca_pe.h>
#include <fstream>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>

using namespace pulsedb;
using Json = nlohmann::json;

struct InitMsgHeader {
    char *remote_mem_addr;
    size_t remote_mem_size;
    size_t remote_mmap_desc_size;
};

struct Options {
    std::string server_name;
    std::string dev_pci;
    std::string dev_rep_pci;
    size_t dma_buf_max_size{};
    size_t dma_size{};
    std::string output_dir;
    size_t repeat_time{};
};

Options read_args(int argc, char **argv) {
    argparse::ArgumentParser program("comch_lat_dpu");

    program.add_argument("--server_name")
        .help("comch server name")
        .required();
    program.add_argument("--dev_pci")
        .help("dpu dev pci")
        .required();
    program.add_argument("--dev_rep_pci")
        .help("dpu dev rep pci")
        .required();
    program.add_argument("--dma_buf_max_size")
        .help("dma buffer max size")
        .required()
        .scan<'d', size_t>();
    program.add_argument("-b")
        .help("dma size")
        .required()
        .scan<'d', size_t>();
    program.add_argument("-o")
        .help("output dir")
        .required();
    program.add_argument("-r")
        .help("repeat time")
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
    opts.dev_rep_pci = program.get<std::string>("--dev_rep_pci");
    opts.dma_buf_max_size = program.get<size_t>("--dma_buf_max_size");
    opts.dma_size = program.get<size_t>("-b");
    opts.output_dir = program.get<std::string>("-o");
    opts.repeat_time = program.get<size_t>("-r");

    if (opts.dma_size > opts.dma_buf_max_size) {
        throw_with("dma_size must be <= dma_buf_max_size");
    }
    return opts;
}

void do_dma(doca_pe *pe, docacxx::Dma &dma, docacxx::Buffer &src, docacxx::Buffer &dst) {
    bool is_finished{};
    auto dma_task = dma.alloc_memcpy_task(src, dst, [&](const auto &) {
        is_finished = true;
    });
    dma.submit(std::move(dma_task));
    while (not is_finished) {
        doca_pe_progress(pe);
    }
}

int main(int argc, char **argv) {
    auto opts = read_args(argc, argv);

    doca_dev *dev{};
    doca_dev_rep *dev_rep{};
    doca_pe *pe{};
    docacxx::check_derr(docacxx::open_doca_device_with_pci(opts.dev_pci.c_str(), &dev),
        "failed to open dev");
    docacxx::check_derr(docacxx::open_doca_device_rep_with_pci(dev, DOCA_DEVINFO_REP_FILTER_NET,
        opts.dev_rep_pci.c_str(), &dev_rep), "failed to open dev rep");
    docacxx::check_derr(doca_pe_create(&pe), "failed to create pe");

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

    docacxx::comch::Server::Cfg server_cfg;
    server_cfg.max_send_task_slot_num = 1;
    docacxx::comch::Server server(opts.server_name.c_str(), dev, dev_rep, pe, server_cfg);
    bool is_sended{};
    server.set_send_cb([&](auto) {
        is_sended = true;
    });
    doca_comch_connection *conn{};
    server.set_new_conn_cb([&](doca_comch_connection *c) {
        conn = c;
    });
    std::unique_ptr<docacxx::Mmap> remote_mmap;
    server.set_recv_cb([&](doca_comch_connection *, uint8_t *data, uint32_t) {
        auto *header = (InitMsgHeader *)data;
        auto *remote_mmap_desc_addr = (char *)(header + 1);
        std::string_view remote_mmap_desc(remote_mmap_desc_addr, header->remote_mmap_desc_size);
        remote_mmap = std::make_unique<docacxx::Mmap>(remote_mmap_desc, dev,
            header->remote_mem_addr, header->remote_mem_size);
    });
    server.start_sync();

    while (not remote_mmap) {
        doca_pe_progress(pe);
    }
    docacxx::Mmap local_mmap(dev, opts.dma_buf_max_size);
    docacxx::BufInventory buf_inv(dev, 2);
    docacxx::Dma::Cfg dma_cfg;
    dma_cfg.memcpy_task_slot_num = 1;
    docacxx::Dma dma(pe, dev, dma_cfg);
    dma.start_sync();

    auto local_buf = buf_inv.get_buf(local_mmap, size_t(0), opts.dma_size);
    auto remote_buf = buf_inv.get_buf(*remote_mmap, size_t(0), opts.dma_size);

    ClockDuration read_time;
    ClockDuration write_time;

    remote_buf->set_doca_buf_data(opts.dma_size);
    for (size_t i{}; i < opts.repeat_time; ++i) {
        local_buf->clear_doca_buf_data();
        read_time += clock([&] {
            do_dma(pe, dma, *remote_buf, *local_buf);
        });
    }

    local_buf->set_doca_buf_data(opts.dma_size);
    for (size_t i{}; i < opts.repeat_time; ++i) {
        remote_buf->clear_doca_buf_data();
        write_time += clock([&] {
            do_dma(pe, dma, *local_buf, *remote_buf);
        });
    }

    dma.clean_sync();

    std::string exit_msg = "Exit";
    auto send_task = server.alloc_send_task(conn, exit_msg.data(), exit_msg.size(), nullptr);
    server.submit(std::move(send_task));
    while (not is_sended) {
        doca_pe_progress(pe);
    }
    server.clean_sync();

    auto output_file = opts.output_dir + "/" + std::to_string(opts.dma_size) + ".json";
    std::ofstream output_fs(output_file);
    if (not output_fs) {
        throw_with("Unable to open " + output_file);
    }

    Json json;
    json["dma_read_lat"] = std::to_string(read_time.count() / (1.0 * opts.repeat_time)) + "ns";
    json["dma_write_lat"] = std::to_string(write_time.count() / (1.0 * opts.repeat_time)) + "ns";

    std::cout << json.dump(4) << std::endl;
    output_fs << json.dump(4) << std::endl;
    output_fs.close();
    return 0;
}
