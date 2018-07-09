#include <cstdlib>

#include "hxhim/Results.hpp"
#include "hxhim/Results_private.hpp"

namespace hxhim {

Results::Result::Result(hxhim_result_type t, const int err, const int db)
    : type(t),
      error(err),
      database(db),
      next(nullptr)
{}

Results::Result::~Result() {
}

Results::Put::Put(const int err, const int db)
    : Result(hxhim_result_type::HXHIM_RESULT_PUT, err, db)
{}

Results::Put::~Put() {}

Results::Get::Get(const int err, const int db,
                  void *sub, std::size_t sub_len,
                  void *pred, std::size_t pred_len,
                  void *obj, std::size_t obj_len)
    : Result(hxhim_result_type::HXHIM_RESULT_GET, err, db),
      subject(sub), subject_len(sub_len),
      predicate(pred), predicate_len(pred_len),
      object(obj), object_len(obj_len)
{}

Results::Get::~Get() {
    ::operator delete(subject);
    ::operator delete(predicate);
    free(object);
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
        Result *next = head->next;
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
    return Valid()?curr->next:nullptr;
}

/**
 * AddPut
 * Appends a put to the list
 *
 * @return the pointer to the new node
 */
Results::Result *Results::AddPut(const int error, const int database) {
    return append(new Put(error, database));
}

/**
 * AddGet
 * Appends a get to the list
 *
 * @return the pointer to the new node
 */
Results::Result *Results::AddGet(const int error, const int database,
                                 void *subject, std::size_t subject_len,
                                 void *predicate, std::size_t predicate_len,
                                 void *object, std::size_t object_len) {
    return append(new Get(error, database, subject, subject_len, predicate, predicate_len, object, object_len));
}

/**
 * AddDelete
 * Appends a delete to the list
 *
 * @return the pointer to the new node
 */
Results::Result *Results::AddDelete(const int error, const int database) {
    return append(new Delete(error, database));
}

/**
 * Append
 * Moves the contents of another hxhim::Results into this one.
 * The other list is emptied out;
 *
 * @return the pointer to the new node
 */
Results &Results::Append(Results *results) {
    if (results) {
        append(results->head);
        tail = results->tail;
        results->head = nullptr;
        results->tail = nullptr;
        results->curr = nullptr;
    }

    return *this;
}

/**
 * append
 * Appends a single result node to the end of the list;
 *
 * @return the pointer to the new node
 */
Results::Result *Results::append(Results::Result *single) {
    (head?tail->next:head) = single;
    return tail = single;
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
        *type = curr->type;
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
        *error = curr->error;
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
        *database = curr->database;
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
    if (curr->type == hxhim_result_type::HXHIM_RESULT_GET) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);

        if (subject) {
            *subject = get->subject;
        }

        if (subject_len) {
            *subject_len = get->subject_len;
        }

        return HXHIM_SUCCESS;
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
    if (curr->type == hxhim_result_type::HXHIM_RESULT_GET) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);

        if (predicate) {
            *predicate = get->predicate;
        }

        if (predicate_len) {
            *predicate_len = get->predicate_len;
        }

        return HXHIM_SUCCESS;
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
    if (curr->type == hxhim_result_type::HXHIM_RESULT_GET) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);

        if (object) {
            *object = get->object;
        }

        if (object_len) {
            *object_len = get->object_len;
        }

        return HXHIM_SUCCESS;
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
