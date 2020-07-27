#include <gtest/gtest.h>

#include "hxhim/private/hxhim.hpp"

/*
 * Test the insertion of operations into HXHIM's queues
 */

const char SUBJECT1[]    = "SUBJECT";
const char PREDICATE1[]  = "PREDICATE";
const char OBJECT1[]     = "OBJECT";

const char SUBJECT2[]    = "SUBJECT";
const char PREDICATE2[]  = "PREDICATE";
const char OBJECT2[]     = "OBJECT";

const hxhim_object_type_t TYPE = HXHIM_OBJECT_TYPE_BYTE;

TEST(Enqueue, PUT) {
    hxhim::Unsent<hxhim::PutData> puts;
    EXPECT_EQ(puts.head, nullptr);
    EXPECT_EQ(puts.tail, nullptr);
    EXPECT_EQ(puts.count, (std::size_t) 0);

    // enqueue one PUT
    EXPECT_EQ(hxhim::PutImpl(puts,
                             construct<ReferenceBlob>((char *) SUBJECT1,   strlen(SUBJECT1)),
                             construct<ReferenceBlob>((char *) PREDICATE1, strlen(PREDICATE1)),
                             TYPE,
                             construct<ReferenceBlob>((char *) OBJECT1,    strlen(OBJECT1))),
              HXHIM_SUCCESS);
    ASSERT_NE(puts.head, nullptr);
    EXPECT_EQ(puts.head, puts.tail);
    EXPECT_EQ(puts.count, (std::size_t) 1);

    {
        hxhim::PutData *head = puts.head;
        EXPECT_EQ(head->subject->data(), SUBJECT1);
        EXPECT_EQ(head->subject->size(), strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->data(), PREDICATE1);
        EXPECT_EQ(head->predicate->size(), strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->object->data(), OBJECT1);
        EXPECT_EQ(head->object->size(), strlen(OBJECT2));
    }

    // enqueue a second PUT
    EXPECT_EQ(hxhim::PutImpl(puts,
                             construct<ReferenceBlob>((char *) SUBJECT2,   strlen(SUBJECT2)),
                             construct<ReferenceBlob>((char *) PREDICATE2, strlen(PREDICATE2)),
                             TYPE,
                             construct<ReferenceBlob>((char *) OBJECT2,    strlen(OBJECT2))),
              HXHIM_SUCCESS);
    ASSERT_NE(puts.head, nullptr);
    EXPECT_NE(puts.head, puts.tail);
    EXPECT_EQ(puts.head->next, puts.tail);
    ASSERT_NE(puts.tail, nullptr);
    EXPECT_EQ(puts.tail->next, nullptr);
    EXPECT_EQ(puts.count, (std::size_t) 2);

    {
        hxhim::PutData *head = puts.head;
        EXPECT_EQ(head->subject->data(), SUBJECT1);
        EXPECT_EQ(head->subject->size(), strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->data(), PREDICATE1);
        EXPECT_EQ(head->predicate->size(), strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->object->data(), OBJECT1);
        EXPECT_EQ(head->object->size(), strlen(OBJECT1));
    }

    {
        hxhim::PutData *tail = puts.tail;
        EXPECT_EQ(tail->subject->data(), SUBJECT2);
        EXPECT_EQ(tail->subject->size(), strlen(SUBJECT2));
        EXPECT_EQ(tail->predicate->data(), PREDICATE2);
        EXPECT_EQ(tail->predicate->size(), strlen(PREDICATE2));
        EXPECT_EQ(tail->object_type, TYPE);
        EXPECT_EQ(tail->object->data(), OBJECT2);
        EXPECT_EQ(tail->object->size(), strlen(OBJECT2));
    }

    destruct(puts.head->subject);
    destruct(puts.head->predicate);
    destruct(puts.head->object);
    destruct(puts.head);
    destruct(puts.tail->subject);
    destruct(puts.tail->predicate);
    destruct(puts.tail->object);
    destruct(puts.tail);
}

TEST(Enqueue, GET) {
    hxhim::Unsent<hxhim::GetData> gets;
    EXPECT_EQ(gets.head, nullptr);
    EXPECT_EQ(gets.tail, nullptr);
    EXPECT_EQ(gets.count, (std::size_t) 0);

    // enqueue one GET
    EXPECT_EQ(hxhim::GetImpl(gets,
                             construct<ReferenceBlob>((char *) SUBJECT1,   strlen(SUBJECT1)),
                             construct<ReferenceBlob>((char *) PREDICATE1, strlen(PREDICATE1)),
                             TYPE),
              HXHIM_SUCCESS);
    ASSERT_NE(gets.head, nullptr);
    EXPECT_EQ(gets.head, gets.tail);
    EXPECT_EQ(gets.count, (std::size_t) 1);

    {
        hxhim::GetData *head = gets.head;
        EXPECT_EQ(head->subject->data(), SUBJECT1);
        EXPECT_EQ(head->subject->size(), strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->data(), PREDICATE1);
        EXPECT_EQ(head->predicate->size(), strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
    }

    // enqueue a second GET
    EXPECT_EQ(hxhim::GetImpl(gets,
                             construct<ReferenceBlob>((char *) SUBJECT2,   strlen(SUBJECT2)),
                             construct<ReferenceBlob>((char *) PREDICATE2, strlen(PREDICATE2)),
                             HXHIM_OBJECT_TYPE_BYTE),
              HXHIM_SUCCESS);
    ASSERT_NE(gets.head, nullptr);
    EXPECT_NE(gets.head, gets.tail);
    EXPECT_EQ(gets.head->next, gets.tail);
    ASSERT_NE(gets.tail, nullptr);
    EXPECT_EQ(gets.tail->next, nullptr);
    EXPECT_EQ(gets.count, (std::size_t) 2);

    {
        hxhim::GetData *head = gets.head;
        EXPECT_EQ(head->subject->data(), SUBJECT1);
        EXPECT_EQ(head->subject->size(), strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->data(), PREDICATE1);
        EXPECT_EQ(head->predicate->size(), strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
    }

    {
        hxhim::GetData *tail = gets.tail;
        EXPECT_EQ(tail->subject->data(), SUBJECT2);
        EXPECT_EQ(tail->subject->size(), strlen(SUBJECT2));
        EXPECT_EQ(tail->predicate->data(), PREDICATE2);
        EXPECT_EQ(tail->predicate->size(), strlen(PREDICATE2));
        EXPECT_EQ(tail->object_type, TYPE);
    }

    destruct(gets.head->subject);
    destruct(gets.head->predicate);
    destruct(gets.head);
    destruct(gets.tail->subject);
    destruct(gets.tail->predicate);
    destruct(gets.tail);
}

TEST(Enqueue, GETOP) {
    const std::size_t num_recs = 10;
    enum hxhim_get_op_t op = HXHIM_GET_EQ;

    hxhim::Unsent<hxhim::GetOpData> getops;
    EXPECT_EQ(getops.head, nullptr);
    EXPECT_EQ(getops.tail, nullptr);
    EXPECT_EQ(getops.count, (std::size_t) 0);

    // enqueue one GETOP
    EXPECT_EQ(hxhim::GetOpImpl(getops,
                               construct<ReferenceBlob>((char *) SUBJECT1,   strlen(SUBJECT1)),
                               construct<ReferenceBlob>((char *) PREDICATE1, strlen(PREDICATE1)),
                               TYPE, num_recs, op),
              HXHIM_SUCCESS);
    ASSERT_NE(getops.head, nullptr);
    EXPECT_EQ(getops.head, getops.tail);
    EXPECT_EQ(getops.count, (std::size_t) 1);

    {
        hxhim::GetOpData *head = getops.head;
        EXPECT_EQ(head->subject->data(), SUBJECT1);
        EXPECT_EQ(head->subject->size(), strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->data(), PREDICATE1);
        EXPECT_EQ(head->predicate->size(), strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->num_recs, num_recs);
        EXPECT_EQ(head->op, op);
    }

    // enqueue a second GETOP
    EXPECT_EQ(hxhim::GetOpImpl(getops,
                               construct<ReferenceBlob>((char *) SUBJECT2,   strlen(SUBJECT2)),
                               construct<ReferenceBlob>((char *) PREDICATE2, strlen(PREDICATE2)),
                               TYPE, num_recs, op),
              HXHIM_SUCCESS);
    ASSERT_NE(getops.head, nullptr);
    EXPECT_NE(getops.head, getops.tail);
    EXPECT_EQ(getops.head->next, getops.tail);
    ASSERT_NE(getops.tail, nullptr);
    EXPECT_EQ(getops.tail->next, nullptr);
    EXPECT_EQ(getops.count, (std::size_t) 2);

    {
        hxhim::GetOpData *head = getops.head;
        EXPECT_EQ(head->subject->data(), SUBJECT1);
        EXPECT_EQ(head->subject->size(), strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->data(), PREDICATE1);
        EXPECT_EQ(head->predicate->size(), strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->num_recs, num_recs);
        EXPECT_EQ(head->op, op);
    }

    {
        hxhim::GetOpData *tail = getops.tail;
        EXPECT_EQ(tail->subject->data(), SUBJECT2);
        EXPECT_EQ(tail->subject->size(), strlen(SUBJECT2));
        EXPECT_EQ(tail->predicate->data(), PREDICATE2);
        EXPECT_EQ(tail->predicate->size(), strlen(PREDICATE2));
        EXPECT_EQ(tail->object_type, TYPE);
        EXPECT_EQ(tail->num_recs, num_recs);
        EXPECT_EQ(tail->op, op);
    }

    destruct(getops.head->subject);
    destruct(getops.head->predicate);
    destruct(getops.head);
    destruct(getops.tail->subject);
    destruct(getops.tail->predicate);
    destruct(getops.tail);
}

TEST(Enqueue, DELETE) {
    hxhim::Unsent<hxhim::DeleteData> deletes;
    EXPECT_EQ(deletes.head, nullptr);
    EXPECT_EQ(deletes.tail, nullptr);
    EXPECT_EQ(deletes.count, (std::size_t) 0);

    // enqueue one DELETE
    EXPECT_EQ(hxhim::DeleteImpl(deletes,
                                construct<ReferenceBlob>((char *) SUBJECT1,   strlen(SUBJECT1)),
                                construct<ReferenceBlob>((char *) PREDICATE1, strlen(PREDICATE1))),
              HXHIM_SUCCESS);
    ASSERT_NE(deletes.head, nullptr);
    EXPECT_EQ(deletes.head, deletes.tail);
    EXPECT_EQ(deletes.count, (std::size_t) 1);

    {
        hxhim::DeleteData *head = deletes.head;
        EXPECT_EQ(head->subject->data(), SUBJECT1);
        EXPECT_EQ(head->subject->size(), strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->data(), PREDICATE1);
        EXPECT_EQ(head->predicate->size(), strlen(PREDICATE1));
    }

    // enqueue a second DELETE
    EXPECT_EQ(hxhim::DeleteImpl(deletes,
                                construct<ReferenceBlob>((char *) SUBJECT2,   strlen(SUBJECT2)),
                                construct<ReferenceBlob>((char *) PREDICATE2, strlen(PREDICATE2))),
              HXHIM_SUCCESS);
    ASSERT_NE(deletes.head, nullptr);
    EXPECT_NE(deletes.head, deletes.tail);
    EXPECT_EQ(deletes.head->next, deletes.tail);
    ASSERT_NE(deletes.tail, nullptr);
    EXPECT_EQ(deletes.tail->next, nullptr);
    EXPECT_EQ(deletes.count, (std::size_t) 2);

    {
        hxhim::DeleteData *head = deletes.head;
        EXPECT_EQ(head->subject->data(), SUBJECT1);
        EXPECT_EQ(head->subject->size(), strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->data(), PREDICATE1);
        EXPECT_EQ(head->predicate->size(), strlen(PREDICATE1));
    }

    {
        hxhim::DeleteData *tail = deletes.tail;
        EXPECT_EQ(tail->subject->data(), SUBJECT2);
        EXPECT_EQ(tail->subject->size(), strlen(SUBJECT2));
        EXPECT_EQ(tail->predicate->data(), PREDICATE2);
        EXPECT_EQ(tail->predicate->size(), strlen(PREDICATE2));
    }

    destruct(deletes.head->subject);
    destruct(deletes.head->predicate);
    destruct(deletes.head);
    destruct(deletes.tail->subject);
    destruct(deletes.tail->predicate);
    destruct(deletes.tail);
}
