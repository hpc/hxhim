#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "hxhim/Blob.hpp"
#include "utils/memory.hpp"

const std::size_t len = 1024;
const hxhim_data_t type = (hxhim_data_t) rand();

TEST(Blob, null) {
    Blob blob;
    EXPECT_EQ(blob.data(), nullptr);
    EXPECT_EQ(blob.size(), 0);

    Blob ref(nullptr, 0, type, false);
    EXPECT_EQ(ref.data(), nullptr);
    EXPECT_EQ(ref.size(), 0);
    EXPECT_EQ(ref.data_type(), type);
    EXPECT_EQ(ref.will_clean(), false);

    Blob real(nullptr, 0, type, true);
    EXPECT_EQ(real.data(), nullptr);
    EXPECT_EQ(real.size(), 0);
    EXPECT_EQ(real.data_type(), type);
    EXPECT_EQ(real.will_clean(), true);
}

TEST(Blob, non_null) {
    void *ptr = alloc(len);

    Blob ref(ptr, len, type, false);
    EXPECT_EQ(ref.data(), ptr);
    EXPECT_EQ(ref.size(), len);
    EXPECT_EQ(ref.data_type(), type);
    EXPECT_EQ(ref.will_clean(), false);

    Blob real(ptr, len, type, true);
    EXPECT_EQ(real.data(), ptr);
    EXPECT_EQ(real.size(), len);
    EXPECT_EQ(real.data_type(), type);
    EXPECT_EQ(real.will_clean(), true);

    // pointer is deallocated automatically
}

TEST(Blob, copy_constructor) {
    // clean == true
    {
        void *ptr = alloc(len);

        Blob src(ptr, len, type, true);
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), true);

        Blob dst(src);
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), false);

        // src doesn't change
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), true);

        // pointer is deallocated automatically
    }

    // clean == false
    {
        void *ptr = alloc(len);

        Blob src(ptr, len, type, false);
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), false);

        Blob dst(src);
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), false);

        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), false);

        dealloc(ptr);
    }
}

TEST(Blob, move_constructor) {
    // clean == true
    {
        void *ptr = alloc(len);

        Blob src(ptr, len, type, true);
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), true);

        Blob dst(std::move(src));
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), true);

        // src is empty
        EXPECT_EQ(src.data(), nullptr);
        EXPECT_EQ(src.size(), 0);
        EXPECT_EQ(src.data_type(), hxhim_data_t::HXHIM_DATA_INVALID);
        EXPECT_EQ(src.will_clean(), false);

        // pointer is deallocated automatically
    }

    // clean == false
    {
        void *ptr = alloc(len);

        Blob src(ptr, len, type, false);
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), false);

        Blob dst(std::move(src));
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), false);

        // src is empty
        EXPECT_EQ(src.data(), nullptr);
        EXPECT_EQ(src.size(), 0);
        EXPECT_EQ(src.data_type(), hxhim_data_t::HXHIM_DATA_INVALID);
        EXPECT_EQ(src.will_clean(), false);

        dealloc(ptr);
    }
}

TEST(Blob, copy_assignment) {
    // clean == true
    {
        void *ptr = alloc(len);

        Blob src(ptr, len, type, true);
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), true);

        Blob dst;
        EXPECT_EQ(dst.data(), nullptr);
        EXPECT_EQ(dst.size(), 0);
        EXPECT_EQ(dst.data_type(), hxhim_data_t::HXHIM_DATA_INVALID);
        EXPECT_EQ(dst.will_clean(), false);

        dst = src;

        // dst gets reference to source
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), false);

        // src doesn't change
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), true);

        // pointer is deallocated automatically
    }

    // clean == false
    {
        void *ptr = alloc(len);

        Blob src(ptr, len, type, false);
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), false);

        Blob dst;
        EXPECT_EQ(dst.data(), nullptr);
        EXPECT_EQ(dst.size(), 0);
        EXPECT_EQ(dst.data_type(), hxhim_data_t::HXHIM_DATA_INVALID);
        EXPECT_EQ(dst.will_clean(), false);

        dst = src;

        // dst gets reference to source
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), false);

        // src doesn't change
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), false);

        dealloc(ptr);
    }
}

TEST(Blob, move_assignment) {
    // clean == true
    {
        void *ptr = alloc(len);

        Blob src(ptr, len, type, true);
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), true);

        Blob dst;
        EXPECT_EQ(dst.data(), nullptr);
        EXPECT_EQ(dst.size(), 0);
        EXPECT_EQ(dst.data_type(), hxhim_data_t::HXHIM_DATA_INVALID);
        EXPECT_EQ(dst.will_clean(), false);

        dst = std::move(src);

        // dst takes ownership of ptr
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), true);

        // src is empty
        EXPECT_EQ(src.data(), nullptr);
        EXPECT_EQ(src.size(), 0);
        EXPECT_EQ(src.data_type(), hxhim_data_t::HXHIM_DATA_INVALID);
        EXPECT_EQ(src.will_clean(), false);

        // pointer is deallocated automatically
    }

    // clean == false
    {
        void *ptr = alloc(len);

        Blob src(ptr, len, type, false);
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), false);

        Blob dst;
        EXPECT_EQ(dst.data(), nullptr);
        EXPECT_EQ(dst.size(), 0);
        EXPECT_EQ(dst.data_type(), hxhim_data_t::HXHIM_DATA_INVALID);
        EXPECT_EQ(dst.will_clean(), false);

        dst = std::move(src);

        // dst gets reference to source
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), false);

        // src is empty
        EXPECT_EQ(src.data(), nullptr);
        EXPECT_EQ(src.size(), 0);
        EXPECT_EQ(src.data_type(), hxhim_data_t::HXHIM_DATA_INVALID);
        EXPECT_EQ(src.will_clean(), false);

        dealloc(ptr);
    }
}

TEST(Blob, pack_unpack) {
    // reference
    {
        void *src_ptr = alloc(len);
        memset(src_ptr, 0, len);

        Blob src(ReferenceBlob(src_ptr, len, type));
        EXPECT_EQ(src.data(), src_ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), false);

        void *packed = alloc(src.pack_size(false));
        char *head = (char *) packed;

        EXPECT_EQ(src.pack(head, false), ((char *) packed) + src.pack_size(false));

        head = (char *) packed;

        Blob dst(nullptr, 0, type, false);
        EXPECT_EQ(dst.unpack(head, false), ((char *) packed) + src.pack_size(false));

        EXPECT_EQ(src.size(), dst.size());
        EXPECT_EQ(memcmp(src.data(), dst.data(), dst.size()), 0);
        EXPECT_EQ(dst.will_clean(), true); // reference was converted to real

        dealloc(packed);
        dealloc(src_ptr);
    }

    // real
    {
        void *src_ptr = alloc(len);
        memset(src_ptr, 0, len);

        Blob src(src_ptr, len, type, true);
        EXPECT_EQ(src.data(), src_ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), true);

        void *packed = alloc(src.pack_size(false));
        char *head = (char *) packed;

        EXPECT_EQ(src.pack(head, false), ((char *) packed) + src.pack_size(false));

        head = (char *) packed;

        Blob dst(nullptr, 0, type, true);
        EXPECT_EQ(dst.unpack(head, false), ((char *) packed) + src.pack_size(false));

        EXPECT_EQ(src.size(), dst.size());
        EXPECT_EQ(memcmp(src.data(), dst.data(), dst.size()), 0);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), true);

        dealloc(packed);
        // src_ptr is deallocated manually
    }
}

TEST(Blob, pack_unpack_ref) {
    // reference
    {
        void *src_ptr = alloc(len);
        memset(src_ptr, 0, len);

        Blob src(ReferenceBlob(src_ptr, len, type));
        EXPECT_EQ(src.data(), src_ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), false);

        void *packed = alloc(src.pack_ref_size(false));
        char *head = (char *) packed;

        EXPECT_EQ(src.pack_ref(head, false), ((char *) packed) + src.pack_ref_size(false));

        head = (char *) packed;

        Blob dst(nullptr, 0, type, false);
        EXPECT_EQ(dst.unpack_ref(head, false), ((char *) packed) + src.pack_ref_size(false));

        EXPECT_EQ(src.size(), dst.size());
        EXPECT_EQ(memcmp(src.data(), dst.data(), dst.size()), 0);
        EXPECT_EQ(dst.will_clean(), false); // reference is still a reference

        dealloc(packed);
        dealloc(src_ptr);
    }

    // real
    {
        void *src_ptr = alloc(len);
        memset(src_ptr, 0, len);

        Blob src(src_ptr, len, type, true);
        EXPECT_EQ(src.data(), src_ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.data_type(), type);
        EXPECT_EQ(src.will_clean(), true);

        void *packed = alloc(src.pack_ref_size(false));
        char *head = (char *) packed;

        EXPECT_EQ(src.pack_ref(head, false), ((char *) packed) + src.pack_ref_size(false));

        head = (char *) packed;

        Blob dst(nullptr, 0, type, true);
        EXPECT_EQ(dst.unpack_ref(head, false), ((char *) packed) + src.pack_ref_size(false));

        EXPECT_EQ(src.size(), dst.size());
        EXPECT_EQ(memcmp(src.data(), dst.data(), dst.size()), 0);
        EXPECT_EQ(dst.data_type(), type);
        EXPECT_EQ(dst.will_clean(), false); // real was converted to reference

        dealloc(packed);
        // src_ptr is deallocated manually
    }
}

TEST(Blob, get) {
    void *ptr = alloc(len);

    Blob ref(ptr, len, type, false);
    EXPECT_EQ(ref.data(), ptr);
    EXPECT_EQ(ref.size(), len);
    EXPECT_EQ(ref.data_type(), type);
    EXPECT_EQ(ref.will_clean(), false);
    {
        void *get_ptr = nullptr;
        std::size_t get_len = 0;
        hxhim_data_t get_type;
        ref.get(&get_ptr, &get_len, &get_type);

        EXPECT_EQ(ptr,  get_ptr);
        EXPECT_EQ(len,  get_len);
        EXPECT_EQ(type, get_type);
    }

    Blob real(ptr, len, type, true);
    EXPECT_EQ(real.data(), ptr);
    EXPECT_EQ(real.size(), len);
    EXPECT_EQ(real.data_type(), type);
    EXPECT_EQ(real.will_clean(), true);
    {
        void *get_ptr = nullptr;
        std::size_t get_len = 0;
        hxhim_data_t get_type;
        ref.get(&get_ptr, &get_len, &get_type);

        EXPECT_EQ(ptr,  get_ptr);
        EXPECT_EQ(len,  get_len);
        EXPECT_EQ(type, get_type);
    }

    // pointer is deallocated automatically
}

TEST(Blob, string) {
    void *str = alloc(len);
    memset(str, 'A', len);

    Blob ref(str, len, type, false);
    EXPECT_EQ((std::string) ref,  std::string(len, 'A'));

    Blob real(str, len, type, true);
    EXPECT_EQ((std::string) real, std::string(len, 'A'));

    // str is deallocated automatically
}

TEST(Blob, comparison) {
    void *ptr = alloc(len);
    memset(ptr, 'A', len);
    memcpy(ptr, "AB", 2); // force the first two bytes to be different to exit memcmp early

    // same
    {
        // same pointer
        EXPECT_EQ(Blob(ptr, len, type), Blob(ptr, len, type));

        // different pointer
        void *ptr2 = alloc(len);
        memcpy(ptr2, ptr, len);
        EXPECT_EQ(Blob(ptr, len, type, false), Blob(ptr2, len, type, true));
    }

    // different
    {
        // pointer
        EXPECT_NE(Blob(ptr, len, type), Blob(((char *) ptr) + 1, len, type));

        // length
        EXPECT_NE(Blob(ptr, len, type), Blob(ptr, len + 1, type));

        // type
        hxhim_data_t type2 = (hxhim_data_t) rand();

        // limit number attempts to get a different type
        for(int i = 0; (i < 5) && (type2 == type); i++) {
            type2 = (hxhim_data_t) rand();
        }

        if (type != type2) {
            EXPECT_NE(Blob(ptr, len, type), Blob(ptr, len, type2));
        }
    }

    // Reference vs Real
    // also cleans up ptr
    EXPECT_EQ(Blob(ptr, len, type, false), Blob(ptr, len, type, true));
}

TEST(Blob, print) {
    Blob blob(alloc(len), len, type, true);

    std::stringstream s;
    EXPECT_NO_THROW(s << blob);

    char open_bracket, comma, close_bracket;
    void *parsed_ptr = nullptr;
    std::size_t parsed_len = 0;
    std::string parsed_type_str;
    bool parsed_clean = false;

    EXPECT_TRUE(s
                >> open_bracket
                >> parsed_ptr >> comma
                >> parsed_len >> comma);
    EXPECT_TRUE(std::getline(s >> std::ws, parsed_type_str, ','));
    EXPECT_TRUE(s
                >> std::boolalpha >> parsed_clean
                >> close_bracket);

    if (type <= hxhim_data_t::HXHIM_DATA_MAX_KNOWN) {
        EXPECT_EQ(parsed_type_str, HXHIM_DATA_STR[type]);
    }
    else {
        std::stringstream unknown_type;
        unknown_type << "Unknown Type (" << type << ")";
        EXPECT_EQ(parsed_type_str, unknown_type.str());
    }

    EXPECT_EQ(Blob(parsed_ptr, parsed_len, type), blob);
    EXPECT_EQ(parsed_clean, blob.will_clean());
}

TEST(Blob, wrappers) {
    // ReferenceBlob constructor
    {
        void *ptr = (void *) (uintptr_t) rand();
        const size_t size = rand();
        const hxhim_data_t type = (hxhim_data_t) rand();

        Blob ref(ReferenceBlob(ptr, size, type));
        EXPECT_EQ(ref.data(), ptr);
        EXPECT_EQ(ref.size(), size);
        EXPECT_EQ(ref.data_type(), type);
        EXPECT_EQ(ref.will_clean(), false);
    }

    // RealBlob (move) constructor
    {
        void *ptr = alloc(len);
        const hxhim_data_t type = (hxhim_data_t) rand();

        Blob real(RealBlob(ptr, len, type));
        EXPECT_EQ(real.data(), ptr);
        EXPECT_EQ(real.size(), len);
        EXPECT_EQ(real.data_type(), type);
        EXPECT_EQ(real.will_clean(), true);
    }

    // ReferenceBlob assignment
    {
        void *ptr = (void *) (uintptr_t) rand();
        const size_t size = rand();
        const hxhim_data_t type = (hxhim_data_t) rand();

        Blob ref;
        EXPECT_EQ(ref.data(), nullptr);
        EXPECT_EQ(ref.size(), 0);
        EXPECT_EQ(ref.data_type(), HXHIM_DATA_INVALID);
        EXPECT_EQ(ref.will_clean(), false);

        ref = ReferenceBlob(ptr, size, type);
        EXPECT_EQ(ref.data(), ptr);
        EXPECT_EQ(ref.size(), size);
        EXPECT_EQ(ref.data_type(), type);
        EXPECT_EQ(ref.will_clean(), false);
    }

    // RealBlob assignment
    {
        void *ptr = alloc(len);
        const hxhim_data_t type = (hxhim_data_t) rand();

        Blob real;
        EXPECT_EQ(real.data(), nullptr);
        EXPECT_EQ(real.size(), 0);
        EXPECT_EQ(real.data_type(), HXHIM_DATA_INVALID);
        EXPECT_EQ(real.will_clean(), false);

        real = RealBlob(ptr, len, type);
        EXPECT_EQ(real.data(), ptr);
        EXPECT_EQ(real.size(), len);
        EXPECT_EQ(real.data_type(), type);
        EXPECT_EQ(real.will_clean(), true);
    }
}
