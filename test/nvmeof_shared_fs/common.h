#include <string>

constexpr size_t kFileSize{4096};
constexpr size_t kFileNum{10}; 
constexpr char kFileBaseName[]{"/home/olh/mnt/local/nvme2n1/testfile"};

constexpr size_t kTestFileId{1};

inline std::string make_content(size_t num) {
    std::string ret;
    std::string num_str{std::to_string(num)};
    while (ret.size() < kFileSize) {
        auto diff = kFileSize - ret.size();
        if (diff >= num_str.size()) {
            ret.append(num_str);
        } else {
            ret.append(num_str.substr(0, diff));
        }
    }
    return ret;
}
