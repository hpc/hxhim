template <typename Blob_t>
Transport::Request::BGetOp <Blob_t>::BGetOp(const std::size_t max_count)
    : Message <Transport::GetOp <Blob_t> > (MessageType::BGET, Direction::REQUEST, max_count)
{}

// type and direction should already be known when calling this function
// otherwise, why is this constructor being called?
template <typename Blob_t>
Transport::Request::BGetOp <Blob_t>::BGetOp(void *buf, std::size_t bufsize)
    : Message <Transport::GetOp <Blob_t> > (buf, bufsize)
{}


template <typename Blob_t>
Transport::Response::BGetOp <Blob_t>::BGetOp(const std::size_t max_count)
    : Message <Transport::GetOp <Blob_t> >(MessageType::BGET, Direction::REQUEST, max_count)
{}

// type and direction should already be known when calling this function
// otherwise, why is this constructor being called?
template <typename Blob_t>
Transport::Response::BGetOp <Blob_t>::BGetOp(void *buf, std::size_t bufsize)
    : Message <Transport::GetOp <Blob_t> >(buf, bufsize)
{}
