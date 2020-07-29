#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "utils/Blob.hpp"
#include "utils/memory.hpp"

const std::size_t len = 1024;

TEST(Blob, null) {
    Blob blob;
    EXPECT_EQ(blob.data(), nullptr);
    EXPECT_EQ(blob.size(), 0);

    Blob ref (ReferenceBlob(nullptr, 0));
    EXPECT_EQ(ref.data(), nullptr);
    EXPECT_EQ(ref.size(), 0);
    EXPECT_EQ(ref.will_clean(), false);

    Blob real(RealBlob(nullptr, 0));
    EXPECT_EQ(real.data(), nullptr);
    EXPECT_EQ(real.size(), 0);
    EXPECT_EQ(real.will_clean(), true);
}

TEST(Blob, non_null) {
    void *ptr = alloc(len);

    Blob ref (ReferenceBlob(ptr, len));
    EXPECT_EQ(ref.data(), ptr);
    EXPECT_EQ(ref.size(), len);
    EXPECT_EQ(ref.will_clean(), false);

    Blob real(RealBlob(ptr, len));
    EXPECT_EQ(real.data(), ptr);
    EXPECT_EQ(real.size(), len);
    EXPECT_EQ(real.will_clean(), true);

    // pointer is deallocated automatically
}

TEST(Blob, assignment) {
    // ref -> ref
    {
        void *ptr = alloc(len);

        Blob src(ReferenceBlob(ptr, len));
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), false);

        Blob dst(ReferenceBlob(nullptr, 0));
        EXPECT_EQ(dst.data(), nullptr);
        EXPECT_EQ(dst.size(), 0);
        EXPECT_EQ(dst.will_clean(), false);

        dst = src;

        // dst gets src
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.will_clean(), false);

        // src doesn't change
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), false);

        dealloc(ptr);
    }

    // real -> real
    {
        void *ptr = alloc(len);

        Blob src(RealBlob(ptr, len));
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), true);

        Blob dst(RealBlob(nullptr, 0));
        EXPECT_EQ(dst.data(), nullptr);
        EXPECT_EQ(dst.size(), 0);
        EXPECT_EQ(dst.will_clean(), true);

        dst = src;

        // dst gets src
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.will_clean(), true);

        // src is nulled
        EXPECT_EQ(src.data(), nullptr);
        EXPECT_EQ(src.size(), 0);
        EXPECT_EQ(src.will_clean(), false);

        // pointer is deallocated automatically
    }

    // ref -> real
    {
        void *ptr = alloc(len);

        Blob src(ReferenceBlob(ptr, len));
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), false);

        Blob dst(RealBlob(nullptr, 0));
        EXPECT_EQ(dst.data(), nullptr);
        EXPECT_EQ(dst.size(), 0);
        EXPECT_EQ(dst.will_clean(), true);

        dst = src;

        // dst gets src
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.will_clean(), false);

        // src doesn't change
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), false);

        // need to deallocate since the real became a reference
        dealloc(ptr);
    }

    // real -> ref
    {
        void *ptr = alloc(len);

        Blob src(RealBlob(ptr, len));
        EXPECT_EQ(src.data(), ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), true);

        Blob dst(ReferenceBlob(nullptr, 0));
        EXPECT_EQ(dst.data(), nullptr);
        EXPECT_EQ(dst.size(), 0);
        EXPECT_EQ(dst.will_clean(), false);

        dst = src;

        // dst gets src
        EXPECT_EQ(dst.data(), ptr);
        EXPECT_EQ(dst.size(), len);
        EXPECT_EQ(dst.will_clean(), true);

        // src is nulled
        EXPECT_EQ(src.data(), nullptr);
        EXPECT_EQ(src.size(), 0);
        EXPECT_EQ(src.will_clean(), false);

        // pointer is deallocated automatically
    }
}


TEST(Blob, pack_unpack) {
    // reference
    {
        void *src_ptr = alloc(len);
        memset(src_ptr, 0, len);

        Blob src(ReferenceBlob(src_ptr, len));
        EXPECT_EQ(src.data(), src_ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), false);

        void *packed = alloc(src.pack_size());
        char *head = (char *) packed;

        EXPECT_EQ(src.pack(head), ((char *) packed) + src.pack_size());

        head = (char *) packed;

        Blob dst(ReferenceBlob(nullptr, 0));
        EXPECT_EQ(dst.unpack(head), ((char *) packed) + src.pack_size());

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

        Blob src(RealBlob(src_ptr, len));
        EXPECT_EQ(src.data(), src_ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), true);

        void *packed = alloc(src.pack_size());
        char *head = (char *) packed;

        EXPECT_EQ(src.pack(head), ((char *) packed) + src.pack_size());

        head = (char *) packed;

        Blob dst(RealBlob(nullptr, 0));
        EXPECT_EQ(dst.unpack(head), ((char *) packed) + src.pack_size());

        EXPECT_EQ(src.size(), dst.size());
        EXPECT_EQ(memcmp(src.data(), dst.data(), dst.size()), 0);
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

        Blob src(ReferenceBlob(src_ptr, len));
        EXPECT_EQ(src.data(), src_ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), false);

        void *packed = alloc(src.pack_ref_size());
        char *head = (char *) packed;

        EXPECT_EQ(src.pack_ref(head), ((char *) packed) + src.pack_ref_size());

        head = (char *) packed;

        Blob dst(ReferenceBlob(nullptr, 0));
        EXPECT_EQ(dst.unpack_ref(head), ((char *) packed) + src.pack_ref_size());

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

        Blob src(RealBlob(src_ptr, len));
        EXPECT_EQ(src.data(), src_ptr);
        EXPECT_EQ(src.size(), len);
        EXPECT_EQ(src.will_clean(), true);

        void *packed = alloc(src.pack_ref_size());
        char *head = (char *) packed;

        EXPECT_EQ(src.pack_ref(head), ((char *) packed) + src.pack_ref_size());

        head = (char *) packed;

        Blob dst(RealBlob(nullptr, 0));
        EXPECT_EQ(dst.unpack_ref(head), ((char *) packed) + src.pack_ref_size());

        EXPECT_EQ(src.size(), dst.size());
        EXPECT_EQ(memcmp(src.data(), dst.data(), dst.size()), 0);
        EXPECT_EQ(dst.will_clean(), false); // real was converted to reference

        dealloc(packed);
        // src_ptr is deallocated manually
    }
}

TEST(Blob, get) {
    void *ptr = alloc(len);

    Blob ref (ReferenceBlob(ptr, len));
    EXPECT_EQ(ref.data(), ptr);
    EXPECT_EQ(ref.size(), len);
    EXPECT_EQ(ref.will_clean(), false);
    {
        void *get_ptr = nullptr;
        std::size_t get_len = 0;
        ref.get(&get_ptr, &get_len);

        EXPECT_EQ(ptr, get_ptr);
        EXPECT_EQ(len, get_len);
    }

    Blob real(RealBlob(ptr, len));
    EXPECT_EQ(real.data(), ptr);
    EXPECT_EQ(real.size(), len);
    EXPECT_EQ(real.will_clean(), true);
    {
        void *get_ptr = nullptr;
        std::size_t get_len = 0;
        ref.get(&get_ptr, &get_len);

        EXPECT_EQ(ptr, get_ptr);
        EXPECT_EQ(len, get_len);
    }

    // pointer is deallocated automatically
}

TEST(Blob, string) {
    void *str = alloc(len);
    memset(str, 'A', len);

    Blob ref (ReferenceBlob(str, len));
    EXPECT_EQ((std::string) ref,  std::string(len, 'A'));

    Blob real(RealBlob(str, len));
    EXPECT_EQ((std::string) real, std::string(len, 'A'));

    // str is deallocated automatically
}
