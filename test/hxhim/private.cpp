#include <gtest/gtest.h>

#include "hxhim/private.hpp"

/*
 * Test the insertion of operations into HXHIM's queues
 */

const char SUBJECT1[]    = "SUBJECT";
const char PREDICATE1[]  = "PREDICATE";
const char OBJECT1[]     = "OBJECT";

const char SUBJECT2[]    = "SUBJECT";
const char PREDICATE2[]  = "PREDICATE";
const char OBJECT2[]     = "OBJECT";

const hxhim_type_t TYPE = HXHIM_BYTE_TYPE;

TEST(Enqueue, PUT) {
    hxhim::Unsent<hxhim::PutData> puts;
    EXPECT_EQ(puts.head, nullptr);
    EXPECT_EQ(puts.tail, nullptr);
    EXPECT_EQ(puts.count, 0);

    // enqueue one PUT
    EXPECT_EQ(hxhim::PutImpl(puts, (char *) SUBJECT1, strlen(SUBJECT1), (char *) PREDICATE1, strlen(PREDICATE1), TYPE, (char *) OBJECT1, strlen(OBJECT1)), HXHIM_SUCCESS);
    ASSERT_NE(puts.head, nullptr);
    EXPECT_EQ(puts.head, puts.tail);
    EXPECT_EQ(puts.count, 1);

    {
        hxhim::PutData *head = puts.head;
        EXPECT_EQ(head->subject->ptr, SUBJECT1);
        EXPECT_EQ(head->subject->len, strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->ptr, PREDICATE1);
        EXPECT_EQ(head->predicate->len, strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->object->ptr, OBJECT1);
        EXPECT_EQ(head->object->len, strlen(OBJECT2));
    }

    // enqueue a second PUT
    EXPECT_EQ(hxhim::PutImpl(puts, (char *) SUBJECT2, strlen(SUBJECT2), (char *) PREDICATE2, strlen(PREDICATE2), TYPE, (char *) OBJECT2, strlen(OBJECT2)), HXHIM_SUCCESS);
    ASSERT_NE(puts.head, nullptr);
    EXPECT_NE(puts.head, puts.tail);
    EXPECT_EQ(puts.head->next, puts.tail);
    ASSERT_NE(puts.tail, nullptr);
    EXPECT_EQ(puts.tail->next, nullptr);
    EXPECT_EQ(puts.count, 2);

    {
        hxhim::PutData *head = puts.head;
        EXPECT_EQ(head->subject->ptr, SUBJECT1);
        EXPECT_EQ(head->subject->len, strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->ptr, PREDICATE1);
        EXPECT_EQ(head->predicate->len, strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->object->ptr, OBJECT1);
        EXPECT_EQ(head->object->len, strlen(OBJECT1));
    }

    {
        hxhim::PutData *tail = puts.tail;
        EXPECT_EQ(tail->subject->ptr, SUBJECT2);
        EXPECT_EQ(tail->subject->len, strlen(SUBJECT2));
        EXPECT_EQ(tail->predicate->ptr, PREDICATE2);
        EXPECT_EQ(tail->predicate->len, strlen(PREDICATE2));
        EXPECT_EQ(tail->object_type, TYPE);
        EXPECT_EQ(tail->object->ptr, OBJECT2);
        EXPECT_EQ(tail->object->len, strlen(OBJECT2));
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
    std::size_t len1 = 0;
    std::size_t len2 = 0;

    hxhim::Unsent<hxhim::GetData> gets;
    EXPECT_EQ(gets.head, nullptr);
    EXPECT_EQ(gets.tail, nullptr);
    EXPECT_EQ(gets.count, 0);

    // enqueue one GET
    EXPECT_EQ(hxhim::GetImpl(gets, (char *) SUBJECT1, strlen(SUBJECT1), (char *) PREDICATE1, strlen(PREDICATE1), TYPE, (char *) OBJECT1, &len1), HXHIM_SUCCESS);
    ASSERT_NE(gets.head, nullptr);
    EXPECT_EQ(gets.head, gets.tail);
    EXPECT_EQ(gets.count, 1);

    {
        hxhim::GetData *head = gets.head;
        EXPECT_EQ(head->subject->ptr, SUBJECT1);
        EXPECT_EQ(head->subject->len, strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->ptr, PREDICATE1);
        EXPECT_EQ(head->predicate->len, strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->object, OBJECT1);
        EXPECT_EQ(head->object_len, &len1);
    }

    // enqueue a second GET
    EXPECT_EQ(hxhim::GetImpl(gets, (char *) SUBJECT2, strlen(SUBJECT2), (char *) PREDICATE2, strlen(PREDICATE2), HXHIM_BYTE_TYPE, (char *) OBJECT2, &len2), HXHIM_SUCCESS);
    ASSERT_NE(gets.head, nullptr);
    EXPECT_NE(gets.head, gets.tail);
    EXPECT_EQ(gets.head->next, gets.tail);
    ASSERT_NE(gets.tail, nullptr);
    EXPECT_EQ(gets.tail->next, nullptr);
    EXPECT_EQ(gets.count, 2);

    {
        hxhim::GetData *head = gets.head;
        EXPECT_EQ(head->subject->ptr, SUBJECT1);
        EXPECT_EQ(head->subject->len, strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->ptr, PREDICATE1);
        EXPECT_EQ(head->predicate->len, strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->object, OBJECT1);
        EXPECT_EQ(head->object_len, &len1);
    }

    {
        hxhim::GetData *tail = gets.tail;
        EXPECT_EQ(tail->subject->ptr, SUBJECT2);
        EXPECT_EQ(tail->subject->len, strlen(SUBJECT2));
        EXPECT_EQ(tail->predicate->ptr, PREDICATE2);
        EXPECT_EQ(tail->predicate->len, strlen(PREDICATE2));
        EXPECT_EQ(tail->object_type, TYPE);
        EXPECT_EQ(tail->object, OBJECT2);
        EXPECT_EQ(tail->object_len, &len2);
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
    EXPECT_EQ(getops.count, 0);

    // enqueue one GETOP
    EXPECT_EQ(hxhim::GetOpImpl(getops, (char *) SUBJECT1, strlen(SUBJECT1), (char *) PREDICATE1, strlen(PREDICATE1), TYPE, num_recs, op), HXHIM_SUCCESS);
    ASSERT_NE(getops.head, nullptr);
    EXPECT_EQ(getops.head, getops.tail);
    EXPECT_EQ(getops.count, 1);

    {
        hxhim::GetOpData *head = getops.head;
        EXPECT_EQ(head->subject->ptr, SUBJECT1);
        EXPECT_EQ(head->subject->len, strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->ptr, PREDICATE1);
        EXPECT_EQ(head->predicate->len, strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->num_recs, num_recs);
        EXPECT_EQ(head->op, op);
    }

    // enqueue a second GETOP
    EXPECT_EQ(hxhim::GetOpImpl(getops, (char *) SUBJECT2, strlen(SUBJECT2), (char *) PREDICATE2, strlen(PREDICATE2), TYPE, num_recs, op), HXHIM_SUCCESS);
    ASSERT_NE(getops.head, nullptr);
    EXPECT_NE(getops.head, getops.tail);
    EXPECT_EQ(getops.head->next, getops.tail);
    ASSERT_NE(getops.tail, nullptr);
    EXPECT_EQ(getops.tail->next, nullptr);
    EXPECT_EQ(getops.count, 2);

    {
        hxhim::GetOpData *head = getops.head;
        EXPECT_EQ(head->subject->ptr, SUBJECT1);
        EXPECT_EQ(head->subject->len, strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->ptr, PREDICATE1);
        EXPECT_EQ(head->predicate->len, strlen(PREDICATE1));
        EXPECT_EQ(head->object_type, TYPE);
        EXPECT_EQ(head->num_recs, num_recs);
        EXPECT_EQ(head->op, op);
    }

    {
        hxhim::GetOpData *tail = getops.tail;
        EXPECT_EQ(tail->subject->ptr, SUBJECT2);
        EXPECT_EQ(tail->subject->len, strlen(SUBJECT2));
        EXPECT_EQ(tail->predicate->ptr, PREDICATE2);
        EXPECT_EQ(tail->predicate->len, strlen(PREDICATE2));
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
    EXPECT_EQ(deletes.count, 0);

    // enqueue one DELETE
    EXPECT_EQ(hxhim::DeleteImpl(deletes, (char *) SUBJECT1, strlen(SUBJECT1), (char *) PREDICATE1, strlen(PREDICATE1)), HXHIM_SUCCESS);
    ASSERT_NE(deletes.head, nullptr);
    EXPECT_EQ(deletes.head, deletes.tail);
    EXPECT_EQ(deletes.count, 1);

    {
        hxhim::DeleteData *head = deletes.head;
        EXPECT_EQ(head->subject->ptr, SUBJECT1);
        EXPECT_EQ(head->subject->len, strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->ptr, PREDICATE1);
        EXPECT_EQ(head->predicate->len, strlen(PREDICATE1));
    }

    // enqueue a second DELETE
    EXPECT_EQ(hxhim::DeleteImpl(deletes, (char *) SUBJECT2, strlen(SUBJECT2), (char *) PREDICATE2, strlen(PREDICATE2)), HXHIM_SUCCESS);
    ASSERT_NE(deletes.head, nullptr);
    EXPECT_NE(deletes.head, deletes.tail);
    EXPECT_EQ(deletes.head->next, deletes.tail);
    ASSERT_NE(deletes.tail, nullptr);
    EXPECT_EQ(deletes.tail->next, nullptr);
    EXPECT_EQ(deletes.count, 2);

    {
        hxhim::DeleteData *head = deletes.head;
        EXPECT_EQ(head->subject->ptr, SUBJECT1);
        EXPECT_EQ(head->subject->len, strlen(SUBJECT1));
        EXPECT_EQ(head->predicate->ptr, PREDICATE1);
        EXPECT_EQ(head->predicate->len, strlen(PREDICATE1));
    }

    {
        hxhim::DeleteData *tail = deletes.tail;
        EXPECT_EQ(tail->subject->ptr, SUBJECT2);
        EXPECT_EQ(tail->subject->len, strlen(SUBJECT2));
        EXPECT_EQ(tail->predicate->ptr, PREDICATE2);
        EXPECT_EQ(tail->predicate->len, strlen(PREDICATE2));
    }

    destruct(deletes.head->subject);
    destruct(deletes.head->predicate);
    destruct(deletes.head);
    destruct(deletes.tail->subject);
    destruct(deletes.tail->predicate);
    destruct(deletes.tail);
}
