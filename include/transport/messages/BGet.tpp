template <typename Blob_t>
Transport::Request::BGet <Blob_t>::BGet(const std::size_t max_count)
    : Message <Transport::SPO <Blob_t> > (Type::BGET, Direction::REQUEST, max_count)
{}

// type and direction should already be known when calling this function
// otherwise, why is this constructor being called?
template <typename Blob_t>
Transport::Request::BGet <Blob_t>::BGet(void *buf, std::size_t bufsize)
    : Message <Transport::SPO <Blob_t> > (buf, bufsize)
{}


template <typename Blob_t>
Transport::Response::BGet <Blob_t>::BGet(const std::size_t max_count)
    : Message <Transport::SP <Blob_t> >(Type::BGET, Direction::REQUEST, max_count)
{}

// type and direction should already be known when calling this function
// otherwise, why is this constructor being called?
template <typename Blob_t>
Transport::Response::BGet <Blob_t>::BGet(void *buf, std::size_t bufsize)
    : Message <Transport::SP <Blob_t> >(buf, bufsize)
{}
