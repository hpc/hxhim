#ifndef HXHIM_TRANSPORT_OPTIONS_PRIVATE
#define HXHIM_TRANSPORT_OPTIONS_PRIVATE

class TransportOptions {
    public:
        TransportOptions(const int type)
            : type_(type)
        {}

         virtual ~TransportOptions() {};

        const int type_;
};

#endif
