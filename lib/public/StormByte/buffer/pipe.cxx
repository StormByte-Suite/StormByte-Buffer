/*
 * Copyright (C) 2024-2026 David C. Manuelda (StormBytePP)
 *
 * This file is part of StormByte-Buffer.
 *
 * StormByte-Buffer original source is dual-licensed:
 *
 * 1. GNU Lesser General Public License v3.0 (or later)
 *    You may redistribute and/or modify this file under the terms of the
 *    GNU Lesser General Public License as published by the Free Software
 *    Foundation, either version 3 of the License, or (at your option)
 *    any later version.
 *
 * 2. Commercial license
 *    Alternatively, this file may be used under the terms of a commercial
 *    license agreement with the copyright holder
 *    (David C. Manuelda <StormByte@gmail.com>).
 *
 * Both licenses apply only to original StormByte-Buffer source in this
 * repository. They do not cover other StormByte modules or any third-party
 * material shipped with this repository (including everything under
 * thirdparty/, and in particular the bundled StormByte-Logger tree and
 * the rest of the StormByte suite it vendors), which remains under its own
 * license.
 *
 * Neither license grants any patent rights. Any patent licenses required
 * to use this software or third-party components must be obtained separately
 * from the patent holders.
 *
 * StormByte-Buffer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * version 3 along with StormByte-Buffer. If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 *
 * SPDX-License-Identifier: LGPL-3.0-or-later OR LicenseRef-StormByte-Commercial
 */

#include <StormByte/buffer/pipe.hxx>

using namespace StormByte::Buffer;

PipeInput::PipeInput(ReadOnly& input) noexcept: m_input(&input) {}

bool PipeInput::Read(const StormByte::ByteSize& count, StormByte::Safe::Binary& data) const noexcept {
	return m_input->Read(count, data);
}

bool PipeInput::EoF() const noexcept {
	return m_input->EoF();
}

StormByte::ByteSize PipeInput::Available() const noexcept {
	return m_input->Available();
}

bool PipeInput::IsReadable() const noexcept {
	return m_input->IsReadable();
}

PipeOutput::PipeOutput(WriteOnly& output) noexcept: m_output(&output) {}

bool PipeOutput::Write(const StormByte::Safe::Binary& data) const noexcept {
	return m_output->Write(data);
}

bool PipeOutput::Write(StormByte::Safe::Binary&& data) const noexcept {
	return m_output->Write(std::move(data));
}

bool PipeOutput::Write(std::string_view text) const noexcept {
	return m_output->Write(text);
}

bool PipeOutput::IsWritable() const noexcept {
	return m_output->IsWritable();
}

void PipeOutput::Close() const noexcept {
	m_output->Close();
}

void PipeOutput::SetError() const noexcept {
	m_output->SetError();
}

Pipe::Pipe(Callback callback) noexcept: m_callback(std::move(callback)) {}

Pipe::Pipe(const Pipe&) = default;

Pipe::Pipe(Pipe&&) noexcept = default;

Pipe::~Pipe() noexcept = default;

Pipe& Pipe::operator=(const Pipe&) = default;

Pipe& Pipe::operator=(Pipe&&) noexcept = default;

StormByte::Safe::Status Pipe::Run(const PipeInput& in, const PipeOutput& out,
	const StormByte::Safe::Shared<StormByte::Logger::Log>& log) const {
	return m_callback.Call(in, out, log);
}
