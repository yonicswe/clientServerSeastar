#include <ctime>
#include <string>
#include <filesystem>
#include <seastar/core/sstring.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/future-util.hh>

namespace defaults{
	const uint16_t server_port = 10000;
	const std::string server_address = "127.0.0.1:10000";
}

namespace utils{

std::string current_time(){
	std::stringstream out;
	std::time_t curr_time = std::time(nullptr);
	std::tm local_time = *std::localtime(&curr_time);
	out << std::put_time(&local_time, "%Y-%m-%d-%H-%M-%S");
	std::string result = out.str();
	return result;
}

struct message{
	uint16_t size;
	union{
		uint32_t number;
		char payload[4];
	};
};

size_t get_io_message_size(const message& msg){
	return sizeof(msg.size) + msg.size;
}

message make_message(int64_t value){
	return {4, {.number = static_cast<uint32_t>(0xFFFFFFFF & value)}};
}

seastar::future<> write_message(seastar::output_stream<char>& out, const message& msg){
	auto written_size = out.write(reinterpret_cast<const char*>(&msg.size), sizeof(msg.size));
	auto written_paylod = written_size.then([&out, &msg]{ return out.write(msg.payload, msg.size); });
	return written_paylod.then([&out]{ return out.flush();});
}

seastar::future<> read_message(seastar::input_stream<char>& in, message& msg){
	auto read_size = in.read_exactly(sizeof(msg.size));
	auto read_payload = read_size.then([&in, &msg](auto buf){ 
		msg.size = *reinterpret_cast<const decltype(msg.size)*>(buf.get()); 
		return in.read_exactly(msg.size);
	});

	return read_payload.then([&msg](auto buf){
		std::copy_n(buf.get(), std::min(buf.size(), sizeof(msg.payload)), msg.payload);
		return seastar::make_ready_future<>();
	});
}

seastar::future<> write_file(std::filesystem::path fpath, seastar::sstring what){
	const size_t DMA_PAGE_SIZE = 4096;
	const size_t fsize = what.size();

	auto buf = seastar::temporary_buffer<char>::aligned(DMA_PAGE_SIZE, seastar::align_up(what.size(), DMA_PAGE_SIZE));
	std::copy(what.begin(), what.end(), buf.get_write());

	return seastar::do_with(std::move(buf), [fpath, fsize](const auto& buf){
		auto opened = seastar::open_file_dma(fpath.c_str(), seastar::open_flags::create|seastar::open_flags::wo);
		return opened.then([&buf, fsize](seastar::file fobj){
			auto written = fobj.dma_write(0, buf.get(), buf.size());
			auto flushed = written.then([fobj](size_t amount) mutable {return fobj.flush();}); //TODO handle error??? 
			auto truncated = flushed.then([fobj, fsize]()mutable{ return fobj.truncate(fsize); });
			auto closed = truncated.then([fobj]() mutable {return fobj.close();});
			return closed.finally([fobj]{ return seastar::make_ready_future<>(); });
		});
	});
}

}//utils
