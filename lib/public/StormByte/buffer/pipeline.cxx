#include <StormByte/buffer/lockfree_ring.hxx>
#include <StormByte/buffer/pipeline.hxx>
#include <StormByte/buffer/producer.hxx>

#include <thread>
#include <vector>

using namespace StormByte::Buffer;

struct Pipeline::Backend {
	std::vector<StormByte::Unique<Pipe>> pipes;
	mutable std::vector<std::unique_ptr<LockFreeRing>> intermediates;
	mutable Producer final_producer;
	mutable std::vector<std::thread> threads;

	/**
	 * @brief Join any running background threads and clear the container.
	 */
	void WaitForCompletion() const noexcept {
		for (auto& t : threads) {
			if (t.joinable())
				t.join();
		}
		threads.clear();
	}
};

Pipeline::Pipeline() noexcept:
	m_io(std::make_unique<Backend>()) {}

Pipeline::Pipeline(const Pipeline& other):
	m_io(std::make_unique<Backend>()) {
	m_io->pipes.reserve(other.m_io->pipes.size());
	for (const auto& pipe : other.m_io->pipes) {
		if (pipe)
			m_io->pipes.push_back(pipe->Clone());
	}
}

Pipeline::Pipeline(Pipeline&& other) noexcept:
	m_io(std::move(other.m_io)) {}

Pipeline::~Pipeline() noexcept {
	if (m_io)
		m_io->WaitForCompletion();
}

Pipeline& Pipeline::operator=(const Pipeline& other) {
	if (this != &other) {
		if (m_io)
			m_io->WaitForCompletion();
		m_io = std::make_unique<Backend>();
		m_io->pipes.reserve(other.m_io->pipes.size());
		for (const auto& pipe : other.m_io->pipes) {
			if (pipe)
				m_io->pipes.push_back(pipe->Clone());
		}
	}
	return *this;
}

Pipeline& Pipeline::operator=(Pipeline&& other) noexcept {
	if (this != &other) {
		if (m_io)
			m_io->WaitForCompletion();
		m_io = std::move(other.m_io);
	}
	return *this;
}

void Pipeline::Add(const Pipe& pipe) {
	m_io->pipes.push_back(pipe.Clone());
}

void Pipeline::Add(Pipe&& pipe) {
	m_io->pipes.push_back(pipe.Move());
}

void Pipeline::SetError() const noexcept {
	for (auto& buf : m_io->intermediates) {
		if (buf)
			buf->SetError();
	}
	m_io->final_producer.SetError();
}

Consumer Pipeline::Process(Consumer buffer,
		const StormByte::Shared<StormByte::Logger::Log>& log,
		const ExecutionMode& mode) const noexcept {
	m_io->WaitForCompletion();

	if (m_io->pipes.empty())
		return buffer;

	const StormByte::Shared<StormByte::Logger::Log> stage_log =
		log ? log->Scope("StormByte/Buffer/Pipeline") : log;

	const std::size_t num_pipes = m_io->pipes.size();

	m_io->intermediates.clear();
	m_io->intermediates.reserve(num_pipes > 1 ? num_pipes - 1 : 0);
	for (std::size_t i = 0; i + 1 < num_pipes; ++i)
		m_io->intermediates.emplace_back(std::make_unique<LockFreeRing>());

	m_io->final_producer = Producer();
	m_io->threads.clear();

	const bool parallel = HasExecutionFlag(mode, ExecutionMode::Parallel);
	const bool async = HasExecutionFlag(mode, ExecutionMode::Async);

	auto run_one = [this, stage_log, num_pipes](const std::size_t i, Consumer& input) {
		ReadOnly& in = (i == 0)
			? static_cast<ReadOnly&>(input)
			: static_cast<ReadOnly&>(*m_io->intermediates[i - 1]);
		WriteOnly& out = (i + 1 == num_pipes)
			? static_cast<WriteOnly&>(m_io->final_producer)
			: static_cast<WriteOnly&>(*m_io->intermediates[i]);
		m_io->pipes[i]->Run(in, out, stage_log);
	};

	auto run_sequential =
		[run_one, buffer = buffer, num_pipes]() mutable {
			for (std::size_t i = 0; i < num_pipes; ++i)
				run_one(i, buffer);
		};

	if (parallel) {
		Consumer input = buffer;
		m_io->threads.reserve(num_pipes);
		for (std::size_t i = 0; i < num_pipes; ++i) {
			m_io->threads.emplace_back(
				[run_one, i, input]() mutable {
					run_one(i, input);
				});
		}
		if (!async)
			m_io->WaitForCompletion();
	}
	else if (async) {
		m_io->threads.emplace_back(std::move(run_sequential));
	}
	else {
		run_sequential();
	}

	return m_io->final_producer.Consumer();
}
