#include <cstdlib>
#include <cstring>

#include "hxhim/private.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/Results_private.hpp"
#include "utils/elen.hpp"

namespace hxhim {

Results::Result::Result(hxhim_result_type t, const int err, const int db)
    : type(t),
      error(err),
      database(db),
      next(nullptr)
{}

Results::Result::~Result() {}

hxhim_result_type_t Results::Result::GetType() const {
    return type;
}

int Results::Result::GetError() const {
    return error;
}

int Results::Result::GetDatabase() const {
    return database;
}

Results::Result *Results::Result::Next() const {
    return next;
}

Results::Result *&Results::Result::Next() {
    return next;
}

Results::Put::Put(const int err, const int db)
    : Result(hxhim_result_type::HXHIM_RESULT_PUT, err, db)
{}

Results::Put::~Put() {}

Results::Get::Get(SPO_Types_t *types, const int err, const int db)
    : Result(hxhim_result_type::HXHIM_RESULT_GET, err, db),
      types(types),
      sub(nullptr), sub_len(0),
      pred(nullptr), pred_len(0),
      obj(nullptr), obj_len(0)
{}

Results::Get::~Get() {
    ::operator delete(sub);
    ::operator delete(pred);
    ::operator delete(obj);
}

/**
 * decode
 * Decodes values taken from a backend back into their
 * local format (i.e. encoded floating point -> double)
 *
 * @param type    the type the data being pointed to by ptr is
 * @param src     the original data
 * @param src_len the original data length
 * @param dst     pointer to the destination buffer of the decoded data
 * @param dst_len pointer to the destination buffer's length
 * @return HXHIM_SUCCESS or HXHIM_ERROR;
 */
int Results::Get::decode(const hxhim_spo_type_t type, void *src, const std::size_t &src_len, void **dst, std::size_t *dst_len) {
    if (!src || !dst || !dst_len) {
        return HXHIM_ERROR;
    }

    *dst = nullptr;
    *dst_len = 0;

    // nothing to decode
    if (!src_len) {
        return HXHIM_SUCCESS;
    }

    switch (type) {
        case HXHIM_SPO_FLOAT_TYPE:
            {
                const float value = elen::decode::floating_point<float>(std::string((char *) src, src_len));
                *dst_len = sizeof(float);
                *dst = ::operator new(*dst_len);
                memcpy(*dst, &value, *dst_len);
            }
            break;
        case HXHIM_SPO_DOUBLE_TYPE:
            {
                const double value = elen::decode::floating_point<double>(std::string((char *) src, src_len));
                *dst_len = sizeof(double);
                *dst = ::operator new(*dst_len);
                memcpy(*dst, &value, *dst_len);
            }
            break;
        case HXHIM_SPO_INT_TYPE:
        case HXHIM_SPO_SIZE_TYPE:
        case HXHIM_SPO_INT64_TYPE:
        case HXHIM_SPO_BYTE_TYPE:
            *dst_len = src_len;
            *dst = ::operator new(*dst_len);
            memcpy(*dst, src, *dst_len);
            break;
        default:
            return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

int Results::Get::GetSubject(void **subject, std::size_t *subject_len) {
    if (FillSubject() != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    if (subject) {
        *subject = sub;
    }

    if (subject_len) {
        *subject_len = sub_len;
    }

    return HXHIM_SUCCESS;
}

int Results::Get::GetPredicate(void **predicate, std::size_t *predicate_len) {
    if (FillPredicate() != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    if (predicate) {
        *predicate = pred;
    }

    if (predicate_len) {
        *predicate_len = pred_len;
    }

    return HXHIM_SUCCESS;
}

int Results::Get::GetObject(void **object, std::size_t *object_len) {
    if (FillObject() != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    if (object) {
        *object = obj;
    }

    if (object_len) {
        *object_len = obj_len;
    }

    return HXHIM_SUCCESS;
}

Results::Delete::Delete(const int err, const int db)
    : Result(hxhim_result_type::HXHIM_RESULT_DEL, err, db)
{}

Results::Delete::~Delete() {}

Results::Results()
    : head(nullptr),
      tail(nullptr),
      curr(nullptr)
{}

Results::~Results() {
    while (head) {
        Result *next = head->Next();
        delete head;
        head = next;
    }
}

/**
 * Valid
 *
 * @return Whether or not the current node is a valid pointer
 */
bool Results::Valid() const {
    return curr;
}

/**
 * GoToHead
 * Moves the current node to point to the head of the list
 *
 * @return the pointer to the head of the list
 */
Results::Result *Results::GoToHead() {
    return curr = head;
}

/**
 * GoToNext
 * Moves the current node to point to the next node in the list
 *
 * @return the pointer to the next node in the list
 */
Results::Result *Results::GoToNext() {
    return curr = Next();
}

/**
 * Curr
 *
 * @return the pointer to the node currently being pointed to
 */
Results::Result *Results::Curr() const {
    return curr;
}

/**
 * Curr
 *
 * @return the pointer to the node after the one currently being pointed to
 */
Results::Result *Results::Next() const {
    return Valid()?curr->Next():nullptr;
}

/**
 * Add
 * Appends a single result node to the end of the list;
 *
 * @return the pointer to the new node
 */
Results::Result *Results::Add(Results::Result *res) {
    (head?tail->Next():head) = res;
    return tail = res;
}

/**
 * Append
 * Moves and appends the contents of another hxhim::Results into this one.
 * The other list is emptied out;
 *
 * @return the pointer to the new node
 */
Results &Results::Append(Results *results) {
    if (results) {
        Add(results->head);
        tail = results->tail;
        results->head = nullptr;
        results->tail = nullptr;
        results->curr = nullptr;
    }

    return *this;
}

}

/**
 * hxhim_results_init
 *
 * @param res A hxhim::Results instance
 * @return the pointer to the C structure containing the hxhim::Results
 */
hxhim_results_t *hxhim_results_init(hxhim::Results *res) {
    hxhim_results_t *ret = new hxhim_results_t();
    ret->res = res;
    return ret;
}

/**
 * hxhim_results_goto_head
 * Moves the internal pointer to the head of the list
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_goto_head(hxhim_results_t *res) {
    if (res && res->res) {
        res->res->GoToHead();
        return HXHIM_SUCCESS;
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_goto_next
 * Moves the internal pointer to the next element in the list
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_goto_next(hxhim_results_t *res) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    res->res->GoToNext();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_valid(hxhim_results_t *res) {
    return (res && res->res && res->res->Valid())?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * hxhim_results_type
 * Gets the type of the result node currently being pointed to
 *
 * @param res   A list of results
 * @param type  (optional) the type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_type(hxhim_results_t *res, enum hxhim_result_type *type) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (!curr) {
        return HXHIM_ERROR;
    }

    if (type) {
        *type = curr->GetType();
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_error
 * Gets the error of the result node currently being pointed to
 *
 * @param res    A list of results
 * @param error  (optional) the error of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_error(hxhim_results_t *res, int *error) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (!curr) {
        return HXHIM_ERROR;
    }

    if (error) {
        *error = curr->GetError();
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_database
 * Gets the database of the result node currently being pointed to
 *
 * @param res       A list of results
 * @param database  (optional) the database of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_database(hxhim_results_t *res, int *database) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (!curr) {
        return HXHIM_ERROR;
    }

    if (database) {
        *database = curr->GetDatabase();
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_get_subject
 * Gets the subject and length from the current result node, if the result node contains data from a GET
 *
 * @param res          A list of results
 * @param subject      (optional) the subject of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_len  (optional) the subject_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_get_subject(hxhim_results_t *res, void **subject, std::size_t *subject_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (curr->GetType() == hxhim_result_type::HXHIM_RESULT_GET) {
        return static_cast<hxhim::Results::Get *>(curr)->GetSubject(subject, subject_len);
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_get_predicate
 * Gets the predicate and length from the current result node, if the result node contains data from a GET
 *
 * @param res            A list of results
 * @param predicate      (optional) the predicate of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_len  (optional) the predicate_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_get_predicate(hxhim_results_t *res, void **predicate, std::size_t *predicate_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (curr->GetType() == hxhim_result_type::HXHIM_RESULT_GET) {
        return static_cast<hxhim::Results::Get *>(curr)->GetPredicate(predicate, predicate_len);
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_get_object
 * Gets the object and length from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object      (optional) the object of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_len  (optional) the object_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_get_object(hxhim_results_t *res, void **object, std::size_t *object_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (curr->GetType() == hxhim_result_type::HXHIM_RESULT_GET) {
        return static_cast<hxhim::Results::Get *>(curr)->GetObject(object, object_len);
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_destroy
 * Destroys the contents of a results struct and the object it is pointing to
 *
 * @param res A list of results
 */
void hxhim_results_destroy(hxhim_results_t *res) {
    if (res) {
        delete res->res;
        delete res;
    }
}
