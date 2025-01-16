#include "config.h"
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

#include <fcntl.h>
#include <stdio.h>

using namespace pulsedb;
using Json = nlohmann::json;

size_t dma_size;
std::string output_dir;
size_t repeat_time;

void init_io_ctx(int &fd, char *&io_buf) {
    fd = open(IO_PATH, O_RDWR | O_CREAT | O_DIRECT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        throw_with("Failed to open {}: {}", IO_PATH, strerror(errno));
    }
    void *addr;
    auto ret = posix_memalign(&addr, BLOCK_SIZE, DMA_BUF_MAX_SIZE);
    if (ret) {
        throw_with("Failed to allocate io buf: {}", strerror(errno));
    }
    io_buf = (char *)addr;
}

void read_args(int argc, char **argv) {
    argparse::ArgumentParser program("dma_with_io");
    
    program.add_argument("-b")
        .help("msg size")
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
    catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        std::exit(1);
    }

    dma_size = program.get<size_t>("-b");
    output_dir = program.get<std::string>("-o");
    repeat_time = program.get<size_t>("-r");

    std::cout << "Dma Size: " << dma_size << std::endl;
    std::cout << "Output Dir: " << output_dir << std::endl;
    std::cout << "Repeat Time: " << repeat_time << std::endl;
}

void dma_write(doca_pe *pe, docacxx::Dma &dma, docacxx::Buffer &src, docacxx::Buffer &dst,
    int fd, char *io_buf) {
    bool is_finished{};
    auto dma_task = dma.alloc_memcpy_task(src, dst, [&](const auto &task) {
        is_finished = true;
    });
    dma.submit(std::move(dma_task));
    while (not is_finished) {
        doca_pe_progress(pe);
    }

    auto ret = write(fd, io_buf, dma_size);
    if (ret != dma_size) {
        throw_with("Write error: {}", strerror(errno));
    }
}

void dma_read(doca_pe *pe, docacxx::Dma &dma, docacxx::Buffer &src, docacxx::Buffer &dst,
    int fd, char *io_buf) {
    bool is_finished{};
    auto dma_task = dma.alloc_memcpy_task(src, dst, [&](const auto &task) {
        is_finished = true;
    });
    dma.submit(std::move(dma_task));
    while (not is_finished) {
        doca_pe_progress(pe);
    }

    auto ret = read(fd, io_buf, dma_size);
    if (ret != dma_size) {
        throw_with("Read error: {}", strerror(errno));
    }
}

int main(int argc, char **argv) {
    read_args(argc, argv);

    int fd{};
    char *io_buf;
    init_io_ctx(fd, io_buf);

    doca_dev *dev{};
    doca_dev_rep *dev_rep{};
    doca_pe *pe{};
    docacxx::check_derr(docacxx::open_doca_device_with_pci(DPU_DEV_PCI, &dev),
        "failed to open dev");
    docacxx::check_derr(docacxx::open_doca_device_rep_with_pci(dev, DOCA_DEVINFO_REP_FILTER_NET,
        DPU_DEV_REP_PCI, &dev_rep), "failed to open dev rep");
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
        free(io_buf);
        close(fd);
    });

    docacxx::comch::Server::Cfg server_cfg;
    server_cfg.max_send_task_slot_num = 1;
    docacxx::comch::Server server(SERVER_NAME, dev, dev_rep, pe, server_cfg);
    bool is_sended{};
    server.set_send_cb([&](auto) {
        is_sended = true;
    });
    doca_comch_connection *conn{};
    server.set_new_conn_cb([&](doca_comch_connection *c) {
        conn = c;
    });
    std::unique_ptr<docacxx::Mmap> remote_mmap;    
    server.set_recv_cb([&](doca_comch_connection *conn, uint8_t *data, uint32_t len) {
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
    docacxx::Mmap local_mmap(dev, io_buf, DMA_BUF_MAX_SIZE);
    docacxx::BufInventory buf_inv(dev, 2);
    docacxx::Dma::Cfg dma_cfg;
    dma_cfg.memcpy_task_slot_num = 1;
    docacxx::Dma dma(pe, dev, dma_cfg);
    dma.start_sync();

    auto local_buf = buf_inv.get_buf(local_mmap, size_t(0), dma_size);
    auto remote_buf = buf_inv.get_buf(*remote_mmap, size_t(0), dma_size);

    ClockDuration read_time;
    ClockDuration write_time;

    local_buf->set_doca_buf_data(dma_size);
    for (size_t i{}; i < repeat_time; ++i) {
        remote_buf->clear_doca_buf_data();
        write_time += clock([&] {
            dma_write(pe, dma, *local_buf, *remote_buf, fd, io_buf);
        });
    }

    if (lseek(fd, 0, SEEK_SET) < 0) {
        throw_with("Failed to lseek: ", strerror(errno));
    }

    remote_buf->set_doca_buf_data(dma_size);
    for (size_t i{}; i < repeat_time; ++i) {
        local_buf->clear_doca_buf_data();
        read_time += clock([&] {
            dma_read(pe, dma, *remote_buf, *local_buf, fd, io_buf);
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

    auto output_file = output_dir + "/" + std::to_string(dma_size) + ".json";
    std::ofstream output_fs(output_file);
    if (not output_fs) {
        throw_with("Unable to open " + output_file);   
    }

    Json json;
    json["dma_with_io_read_lat"] = std::to_string(read_time.count() / (1.0 * repeat_time)) + "ns";
    json["dma_with_io_write_lat"] = std::to_string(write_time.count() / (1.0 * repeat_time)) + "ns";

    std::cout << json.dump(4) << std::endl;
    output_fs << json.dump(4) << std::endl;
    output_fs.close();
    return 0;
}
