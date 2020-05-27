#include <vector>
#include <numeric>
#include <iostream>
#include <algorithm>
#include <type_traits>
#include <seastar/core/gate.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/distributed.hh>


namespace bpo = boost::program_options;

class client;
seastar::distributed<client> clients;

seastar::logger lg("client");


namespace utils{

template<typename TContainer>
std::ostream& print_histogram(std::ostream& out, std::string header, const TContainer& container) {
	static_assert(std::is_unsigned<typename TContainer::value_type>::value);
	out << header;
	auto max_value = *std::max_element(container.begin(), container.end());
	if (!max_value){
		return out << std::endl << "  bin[*] = 0";
	}

	typename TContainer::value_type seen_so_far = {};
	auto total = std::accumulate(container.begin(), container.end(), 0);

	for(size_t bin_idx = 0; bin_idx < container.size(); ++bin_idx){
		if(container[bin_idx]){
			seen_so_far += container[bin_idx];
			const uint16_t percentage = 100*(long double)(seen_so_far)/(long double)(total);
			out << std::endl << "  bin[" << bin_idx << "] = " << container[bin_idx] << " " << percentage << "%";
		}
	}
	return out;
}
}

struct statistics{
	statistics()
		: latency(32, 0)
	{}

	friend std::ostream& operator<<(std::ostream& out, const statistics& stats){
		auto elapsed = stats.finished - stats.started;
		auto usecs = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
		auto secs = static_cast<double>(usecs) / static_cast<double>(1000 * 1000);
		auto requests_per_seconds = static_cast<double>(stats.n_requests)/secs;

		out << "duration=" << usecs << " micro, "
			<< "bytes(w=" << stats.bytes_written << ", r=" << stats.bytes_read << "), "
			<< "requests=(total=" << stats.n_requests << ", rate=" << requests_per_seconds << "), "
			<< "responses=" << stats.n_responses 
			<< std::endl;

		utils::print_histogram(out, "request/response latency (microseconds)", stats.latency);

		return out;
	}

	statistics& operator+=(const statistics& other){
		bytes_read += other.bytes_read;
		bytes_written += other.bytes_written;
		n_requests += other.n_requests;
		n_responses += other.n_responses;
		started = std::min(started, other.started);
		finished = std::max(finished, other.finished);
		for(size_t idx = 0; idx < latency.size(); ++idx){
			latency[idx] += other.latency[idx];
		}
		return *this;
	}

	seastar::future<> freeze(){
		finished = seastar::lowres_clock::now();
		return seastar::make_ready_future<>();
	}

	void write(uint64_t bytes){
		bytes_written += bytes;
		n_requests += 1;
	}

	void read(uint64_t bytes, std::chrono::microseconds rr_duration){
		bytes_read += bytes;
		n_responses += 1;
		latency[std::min(latency.size() - 1, size_t(rr_duration.count()))] += 1;
	}

	uint64_t bytes_read = 0;
	uint64_t bytes_written = 0;
	uint64_t n_requests = 0;
	uint64_t n_responses = 0;
	seastar::lowres_clock::time_point started = seastar::lowres_clock::now();
	seastar::lowres_clock::time_point finished = seastar::lowres_clock::now();
	std::vector<uint64_t> latency = std::vector<uint64_t>(32,0);
};


struct connection {
	explicit connection(size_t idx, seastar::connected_socket&& fd)
	: _idx(idx)
	  , _fd(std::move(fd))
	  , _read_buf(_fd.input())
	  , _write_buf(_fd.output())
	{}

	seastar::future<> send(int64_t times){
		_stats = statistics();

		auto loop = seastar::do_for_each(boost::counting_iterator<int64_t>(0)
										, boost::counting_iterator<int64_t>(times)
										, [this](auto loop_idx){return send1(loop_idx);});

		return loop.finally([this]{ return _stats.freeze(); });
	}

	statistics stats() const { return _stats; }

private:

	seastar::future<> send1(int64_t loop_idx){
		auto started = seastar::lowres_clock::now();
		lg.debug("client connect_and_talk - connection({}) - loop({}) - writing data", _idx, loop_idx);
		return _write_buf.write("ping")
			   .then([this] {
					return _write_buf.flush();
				})
			   .then([this, started, loop_idx] {
					_stats.write(4);
					lg.debug("client connect_and_talk - connection({}) - loop({}) - writing data - done", _idx, loop_idx);
					return _read_buf.read_exactly(4)
						   .then([this, started, loop_idx] (seastar::temporary_buffer<char> buf) {
								lg.debug("client connect_and_talk - connection({}) - loop({}) - reading data - done({})", _idx, loop_idx, buf.size());
								auto duration = std::chrono::microseconds(seastar::lowres_clock::now() - started);
								_stats.read(buf.size(), duration);
								if (buf.size() != 4) {
									lg.error("client connect_and_talk - connection({}) - loop({}) - reading data - done({} != 4)", _idx, loop_idx, buf.size());
									return seastar::make_ready_future<>();
								}
								auto str = std::string(buf.get(), buf.size());
								if (str != "ping") {
									lg.error("client connect_and_talk - connection({}) - loop({}) - reading data - done({} != ping)", _idx, loop_idx, str);
									return seastar::make_ready_future<>();
								}
								return seastar::make_ready_future<>();
							});
				});
	}

private:
	const size_t _idx;
	seastar::connected_socket _fd;
	seastar::input_stream<char> _read_buf;
	seastar::output_stream<char> _write_buf;
	statistics _stats;
};


class client {
public:
	seastar::future<> start(bpo::variables_map config){
		_server_addr = seastar::ipv4_addr{config["server"].as<std::string>()};
		_concurrent_connections = seastar::smp::count * config["connections"].as<uint16_t>();
		_requests_per_connection = config["requests_per_connection"].as<uint32_t>();

		lg.debug("client issue connect_and_talk");

		auto loop = seastar::parallel_for_each( boost::counting_iterator<uint16_t>(0)
												, boost::counting_iterator<uint16_t>(_concurrent_connections)
												, [this](size_t idx){ return connect_and_talk(idx);});
		return loop.finally([this]{ return report_stats(); });
    }

    seastar::future<> stop() {
        return seastar::make_ready_future();
    }

private:
	seastar::future<> connect_and_talk(size_t idx){
		seastar::socket_address local = seastar::socket_address(::sockaddr_in{AF_INET, INADDR_ANY, {0}});
		auto socket = seastar::connect(make_ipv4_address(_server_addr), local, seastar::transport::TCP);
		return socket.then([this, idx] (seastar::connected_socket fd) {
				lg.debug("client connect_and_talk - connected({})", idx);
                auto conn = seastar::make_lw_shared<connection>(idx, std::move(fd));
				auto sent = conn->send(_requests_per_connection);
				return sent.then_wrapped([this, conn, idx] (auto&& f) {
					try {
						f.get();
						lg.debug("client connect_and_talk - connection({}) sent all data(ok)", idx);
						_stats[idx] = conn->stats();
					} catch (std::exception& ex) {
						lg.error("client connect_and_talk - connection({}) sent all data(error={})", idx, ex.what());
					}
					return seastar::make_ready_future<>();
                });
        });
	}

	seastar::future<> report_stats() {
		statistics total;
		for(const auto& [idx, stats] : _stats){
			std::cout << "idx=" << idx << " " << stats << std::endl;
			total += stats;
		}

		std::cout << "total " << total << std::endl;
		return seastar::make_ready_future<>();
    }

private:
    uint16_t _concurrent_connections;
	int64_t _requests_per_connection;
    seastar::ipv4_addr _server_addr;
	seastar::lowres_clock::time_point _earliest_started;
    seastar::lowres_clock::time_point _latest_finished;
    unsigned _num_reported;
	std::map<size_t, statistics> _stats;
};


int main(int ac, char ** av) {
    seastar::app_template app;
    app.add_options()
        ("server", bpo::value<std::string>()->required(), "server address")
        ("connections", bpo::value<uint16_t>()->default_value(16), "number of connections per cpu")
        ("requests_per_connection", bpo::value<uint32_t>()->default_value(10000), "requests per connection")
        ;

    return app.run(ac, av, [&app]{
		auto& config = app.configuration();

		auto started = clients.start().then([&config] () {
			return clients.invoke_on_all(seastar::smp_submit_to_options(), &client::start, config);
		});
		return started.then([]{return clients.stop();});
	});
}
//strace -s 1024 -o client.txt ./client --server 127.0.0.1:10000 -m 20M -c 1 --cpuset 1 --requests_per_connection=100 --connections 4 --default-log-level trace
