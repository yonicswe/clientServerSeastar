#include <ctime>
#include <array>
#include <chrono>
#include <random>
#include <numeric>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <filesystem>
#include <type_traits>
#include <seastar/core/align.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>

#include "common.h"

namespace cs = seastar;

cs::logger lg("client");

std::random_device randdev;
std::default_random_engine randeng(randdev()); //replace randdev with hardcoded number to get same sequence
std::uniform_int_distribution<uint8_t> sleep_between_msgs_dist(0, 10);


namespace bpo = boost::program_options;

class client;
cs::distributed<client> clients;


struct statistics{
	typedef std::array<uint64_t, 2048> latency_hist_t;

	cs::sstring format_general() const{
		auto duration_us = std::max(1L, std::chrono::duration_cast<std::chrono::microseconds>(_finished - _started).count());
		//auto duration_s = std::max(1.0, static_cast<double>(duration_us) / static_cast<double>(1000 * 1000));
		//auto throughput = static_cast<double>(_n_requests)/duration_s;
		//auto bandwidth = static_cast<double>(_bytes_written+_bytes_read)/duration_s;

		return cs::format( "bytes_written,bytes_read,n_requests,n_responses,duration_us\r\n"
						   "{},{},{},{},{}\r\n"
						   , _bytes_written,_bytes_read,_n_requests,_n_responses,duration_us);
	}
			
	cs::sstring format_latency() const{
		cs::sstring result;
		result += cs::sstring("latency_us,messages\r\n");
		for(size_t ltnc = 0; ltnc < _latency.size(); ++ltnc){
			auto msgs = _latency[ltnc];
			if(msgs){
				result += cs::format("{},{}\r\n", ltnc, msgs);
			}
		}
		return result;
	}

	cs::future<> freeze(){
		_finished = cs::lowres_clock::now();
		return cs::make_ready_future<>();
	}

	void write(uint64_t bytes){
		_bytes_written += bytes;
		_n_requests += 1;
	}

	void read(uint64_t bytes, std::chrono::microseconds rr_duration){
		_bytes_read += bytes;
		_n_responses += 1;
		_latency[std::min(_latency.size() - 1, size_t(rr_duration.count()))] += 1;
	}

private:
	uint64_t _bytes_read = 0;
	uint64_t _bytes_written = 0;
	uint64_t _n_requests = 0;
	uint64_t _n_responses = 0;
	latency_hist_t _latency = {};
	cs::lowres_clock::time_point _started = cs::lowres_clock::now();
	cs::lowres_clock::time_point _finished = cs::lowres_clock::now();
};


struct connection {
	struct configuration{
		configuration change_id(size_t id) const {
			auto cfg = *this;
			cfg.id = id;
			return cfg;
		}

		size_t id;
		int64_t n_msgs;
		bool sleep_between_msgs;
	};

	explicit connection(configuration cfg, cs::connected_socket&& fd)
	: _cfg(cfg)
	  , _fd(std::move(fd))
	  , _read_buf(_fd.input())
	  , _write_buf(_fd.output())
	{}

	cs::future<> send(){
		_stats = statistics();

		auto loop = cs::do_for_each(boost::counting_iterator<int64_t>(0)
									, boost::counting_iterator<int64_t>(_cfg.n_msgs)
									, [this](auto loop_idx){return send1(loop_idx);});

		return loop.finally([this]{ return _stats.freeze(); });
	}

	statistics stats() const { return _stats; }

private:

	cs::future<> send1(int64_t loop_idx){
		_write_msg = utils::make_message(loop_idx);
		auto started = std::chrono::steady_clock::now();
		auto written = utils::write_message(_write_buf, _write_msg);
		auto read = written.then([this] { 
			_stats.write(utils::get_io_message_size(_write_msg));
			if (_cfg.sleep_between_msgs){
				auto period = std::chrono::microseconds(sleep_between_msgs_dist(randeng));
				return cs::sleep(period).then([this, period]{ return utils::read_message(_read_buf, _read_msg); });
			} else {
				return utils::read_message(_read_buf, _read_msg);
			}
		});
		return read.then([this, started]{
			auto finished = std::chrono::steady_clock::now();
			auto duration_fine = std::chrono::nanoseconds(finished - started); //steady clock is using nanoseconds
			auto duration = std::chrono::microseconds(duration_fine.count() / 1000);
			_stats.read(utils::get_io_message_size(_read_msg), duration);
			if (_read_msg.number != _write_msg.number){
				lg.error("client connect_and_talk - connection({}) - read({}) != written({})", _cfg.id, _read_msg.number, _write_msg.number);
			}
			return cs::make_ready_future<>();
		});
	}

private:
	const configuration _cfg;
	cs::connected_socket _fd;
	cs::input_stream<char> _read_buf;
	cs::output_stream<char> _write_buf;
	utils::message _read_msg;
	utils::message _write_msg;
	statistics _stats;
};


class client {
public:
	cs::future<> start(bpo::variables_map config){
		_shard_id = cs::this_shard_id();
		_server_addr = cs::ipv4_addr{config["server"].as<std::string>()};
		_working_dir = config["working_dir"].as<std::filesystem::path>();
		auto _connections = cs::smp::count * config["connections"].as<uint16_t>();

		_common_cfg.n_msgs = config["msgs_per_connection"].as<uint32_t>();
		_common_cfg.sleep_between_msgs = config["sleep_between_msgs"].as<bool>();

		lg.debug("client issue connect_and_talk");

		auto loop = cs::parallel_for_each( boost::counting_iterator<uint16_t>(0)
										   , boost::counting_iterator<uint16_t>(_connections)
										   , [this](size_t idx){ return connect_and_talk(idx);});

		return loop.then([this]{
			auto dump = [this](const auto& idx2stats){ return dump_stats(idx2stats.first, idx2stats.second); };
			return cs::parallel_for_each(_stats.begin(), _stats.end(), dump);
		});
    }

    cs::future<> stop() {
        return cs::make_ready_future();
    }

private:
	cs::future<> connect_and_talk(size_t idx){
		cs::socket_address local = cs::socket_address(::sockaddr_in{AF_INET, INADDR_ANY, {0}});
		auto socket = cs::connect(_server_addr, local, cs::transport::TCP);
		return socket.then([this, idx] (cs::connected_socket fd) {
				lg.debug("client connect_and_talk - connected({})", idx);
                auto conn = cs::make_lw_shared<connection>(_common_cfg.change_id(idx), std::move(fd));
				auto sent = conn->send();
				return sent.then_wrapped([this, conn, idx] (auto&& f) {
					try {
						f.get();
						lg.info("client connect_and_talk - connection({}) sent all data(ok)", idx);
						_stats[idx] = conn->stats();
					} catch (std::exception& ex) {
						lg.error("client connect_and_talk - connection({}) sent all data(error={})", idx, ex.what());
					}
					return cs::make_ready_future<>();
                });
        });
	}

	cs::future<> dump_stats(const size_t idx, const statistics& conn_stats){
		auto general_fname = _working_dir / cs::format("client_{}_conn_{}_general.csv", _shard_id, idx).c_str();
		auto latency_fname = _working_dir / cs::format("client_{}_conn_{}_latency.csv", _shard_id, idx).c_str();

		auto general = utils::write_file(general_fname, conn_stats.format_general());
		auto latency = utils::write_file(latency_fname, conn_stats.format_latency());
		return cs::when_all_succeed(std::move(general), std::move(latency))
			   .then_wrapped([](auto unused){
					return cs::make_ready_future<>();
			   });
	}

private:
	cs::shard_id _shard_id;
    cs::ipv4_addr _server_addr;
	connection::configuration _common_cfg;
	std::filesystem::path _working_dir;
	std::map<size_t, statistics> _stats;
};


int main(int ac, char ** av) {
    cs::app_template app;
    app.add_options()
		//small numbers and default options for development, invocation scripts will be verbose
        ("server", bpo::value<std::string>()->default_value(defaults::server_address), "server address")
        ("connections", bpo::value<uint16_t>()->default_value(8), "number of connections per cpu")
        ("msgs_per_connection", bpo::value<uint32_t>()->default_value(1000), "messages per connection")
        ("sleep_between_msgs", bpo::value<bool>()->default_value(false), "sleep random period (us) between sent messages")
		("working_dir", bpo::value<std::filesystem::path>()->default_value(std::filesystem::path(utils::current_time())), "the directory to store test relevant information");
        ;

	std::stringstream arguments;
	std::copy(av, av+ac, std::ostream_iterator<std::string>(arguments, " "));

    return app.run(ac, av, [&app, &arguments]{

		auto& config = app.configuration();
		std::filesystem::path working_dir = config["working_dir"].as<std::filesystem::path>();

		std::srand(std::time(nullptr));

		auto touch_wdir = cs::recursive_touch_directory(working_dir.c_str());
		auto saved_args = touch_wdir.then([working_dir, &arguments]{ return utils::write_file(working_dir / "args.txt", arguments.str()); });

		return saved_args.then([&config]{

			auto started = clients.start().then([&config] () {
				return clients.invoke_on_all(cs::smp_submit_to_options(), &client::start, config);
			});
			return started.then([]{return clients.stop();});
		});
	});
}

//clang++ -o client -ggdb client.cpp $(pkg-config --libs --cflags --static ~/development/seastar/build/debug/seastar.pc)
//./client --memory 20M --smp 1 --cpuset 1
//./client --memory 20M --smp 1 --cpuset 1 --default-log-level trace --server 127.0.0.1:10000 --msgs_per_connection=100 --connections 4 
