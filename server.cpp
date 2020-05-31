#include "seastar/core/pipe.hh"
#include "seastar/core/seastar.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/app-template.hh"
#include "boost/program_options.hpp"
#include "common.h"


namespace cs = seastar;

cs::logger lg("server");

cs::future<> handle_connection(cs::accept_result accepted) {
	auto out = accepted.connection.output();
	auto in = accepted.connection.input();
	auto done = do_with(std::move(accepted), std::move(out), std::move(in), utils::message{},
    			   [](auto &s, auto &out, auto &in, auto& msg) {
        return cs::repeat([&out, &in, &msg] {
				auto read = utils::read_message(in, msg);
				auto echo = read.then([&out, &msg]{ return utils::write_message(out, msg); });
				return echo.then([]{ return cs::make_ready_future<cs::stop_iteration>(cs::stop_iteration::no); });
	   	});
    });

	auto remote_address = accepted.remote_address;
	return done.handle_exception([remote_address](std::exception_ptr err){
		try{
			std::rethrow_exception(err);
		}
		catch(cs::broken_pipe_exception& err){
			lg.info("connection closed: {} - {}", remote_address, err.what());
		} catch(std::exception& err){
			lg.error("error: {} -{}", remote_address, err.what());
		}
		return cs::make_ready_future<>();
	});
}


cs::future<> service_loop(uint16_t port) {
	const cs::listen_options lo = {.reuse_address = true};
	return cs::do_with(cs::listen(cs::make_ipv4_address({port}), lo), [](auto &listener) {
			return cs::keep_doing([&listener]() {
				return listener.accept().then(
					[](cs::accept_result accepted) {
					// Note we ignore, not return, the future returned by
					// handle_connection(), so we do not wait for one
					// connection to be handled before accepting the next one.
					lg.info("connection accepted - {}", accepted.remote_address);
					auto done = handle_connection(std::move(accepted));
				});
			});
	});
}


cs::future<> start_service_loop_on_core(cs::app_template& app, uint16_t core){
	auto& args = app.configuration();
	uint16_t port = args["port"].as<uint16_t>();
	if (args["port_per_core"].as<bool>())
		port += core;
	return cs::smp::submit_to(core, [port](){ return service_loop(port); });
}


cs::future<> start_service_loop(cs::app_template& app) {
	cs::engine().at_exit([](){::_exit(0); return cs::make_ready_future<>(); }); //This is Sparta!!!
	auto smp_submit = [&app](uint16_t core){ return start_service_loop_on_core(app, core); }; 
	return cs::parallel_for_each(boost::irange<unsigned>(0, cs::smp::count), smp_submit);
}


int main(int argc, char** argv){
	namespace bpo = boost::program_options;
	cs::app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(defaults::server_port), "server port")
        ("port_per_core", bpo::value<bool>()->default_value(true), "use unique port per core");
	return app.run(argc, argv, [&app](){return start_service_loop(app);});
}

//clang++ -o server -ggdb server.cpp $(pkg-config --libs --cflags --static ~/development/seastar/build/debug/seastar.pc)

