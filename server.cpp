#include "seastar/core/seastar.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/app-template.hh"
#include "boost/program_options.hpp"


seastar::future<> handle_connection(seastar::accept_result accepted) {
  auto out = accepted.connection.output();
  auto in = accepted.connection.input();
  return do_with( std::move(accepted), std::move(out), std::move(in),
      [](auto &s, auto &out, auto &in) {
        return seastar::repeat([&out, &in] {
                 return in.read_exactly(4).then([&out](auto buf) {
		   int x;
		   x = 1;
		   x++;
                   if (buf) {
                     return out.write(std::move(buf))
                         .then([&out] { return out.flush(); })
                         .then([] { return seastar::stop_iteration::no; });
                   } else {
                     return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                   }
                 });
               })
            .then([&out] { return out.close(); });
      });
}


seastar::future<> service_loop(uint16_t port) {
	const seastar::listen_options lo = {.reuse_address = true};
	return seastar::do_with(seastar::listen(seastar::make_ipv4_address({port}), lo), [](auto &listener) {
			return seastar::keep_doing([&listener]() {
				return listener.accept().then(
					[](seastar::accept_result accepted) {
					// Note we ignore, not return, the future returned by
					// handle_connection(), so we do not wait for one
					// connection to be handled before accepting the next one.
					auto done = handle_connection(std::move(accepted));
				});
			});
	});
}


seastar::future<> start_service_loop_on_core(seastar::app_template& app, uint16_t core){
	auto& args = app.configuration();
	uint16_t port = args["port"].as<uint16_t>();
	if (args["port_per_core"].as<bool>())
		port += core;
	return seastar::smp::submit_to(core, [port](){ return service_loop(port); });
}


seastar::future<> start_service_loop(seastar::app_template& app) {
	seastar::engine().at_exit([](){::_exit(0); return seastar::make_ready_future<>(); }); //This is Sparta!!!

	auto smp_submit = [&app](uint16_t core){ return start_service_loop_on_core(app, core); };


	return seastar::parallel_for_each(boost::irange<unsigned>(0, seastar::smp::count), smp_submit);
}


int main(int argc, char** argv){
	namespace bpo = boost::program_options;
	seastar::app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "server port")
        ("port_per_core", bpo::value<bool>()->default_value(true), "use unique port per core");
	return app.run(argc, argv, [&app](){return start_service_loop(app);});
}

//clang++ -o server -ggdb server.cpp $(pkg-config --libs --cflags --static ~/devel/seastar/build/debug/seastar.pc)

