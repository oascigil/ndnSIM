/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/**
 * Copyright (c) 2014,  Regents of the University of California,
 *                      Arizona Board of Regents,
 *                      Colorado State University,
 *                      University Pierre & Marie Curie, Sorbonne University,
 *                      Washington University in St. Louis,
 *                      Beijing Institute of Technology,
 *                      The University of Memphis
 *
 * This file is part of NFD (Named Data Networking Forwarding Daemon).
 * See AUTHORS.md for complete list of NFD authors and contributors.
 *
 * NFD is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * NFD is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * NFD, e.g., in COPYING.md file.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "random-load-balancer-strategy.hpp"

#include <boost/random/uniform_int_distribution.hpp>

#include <ndn-cxx/util/random.hpp>

#include "core/logger.hpp"

NFD_LOG_INIT("RandomLoadBalancerStrategy");

namespace nfd {
namespace fw {

const Name
  RandomLoadBalancerStrategy::STRATEGY_NAME("ndn:/localhost/nfd/strategy/random-load-balancer");

RandomLoadBalancerStrategy::RandomLoadBalancerStrategy(Forwarder& forwarder, const Name& name)
  : Strategy(forwarder, name)
{
}

RandomLoadBalancerStrategy::~RandomLoadBalancerStrategy()
{
}

static bool
canForwardToNextHop(shared_ptr<pit::Entry> pitEntry, const fib::NextHop& nexthop)
{
  return pitEntry->canForwardTo(*nexthop.getFace());
}

static bool
hasFaceForForwarding(const fib::NextHopList& nexthops, shared_ptr<pit::Entry>& pitEntry)
{
  return std::find_if(nexthops.begin(), nexthops.end(), bind(&canForwardToNextHop, pitEntry, _1))
         != nexthops.end();
}

void
RandomLoadBalancerStrategy::afterReceiveInterest(const Face& inFace, const Interest& interest,
                                                 shared_ptr<fib::Entry> fibEntry,
                                                 shared_ptr<pit::Entry> pitEntry)
{
  NFD_LOG_TRACE("afterReceiveInterest");

  if (pitEntry->hasUnexpiredOutRecords()) {
    // not a new Interest, don't forward
    return;
  }

  const fib::NextHopList& nexthops = fibEntry->getNextHops();

  // Ensure there is at least 1 Face is available for forwarding
  if (!hasFaceForForwarding(nexthops, pitEntry)) {
    this->rejectPendingInterest(pitEntry);
    return;
  }

  fib::NextHopList::const_iterator selected;
  do {
    boost::random::uniform_int_distribution<> dist(0, nexthops.size() - 1);
    const size_t randomIndex = dist(m_randomGenerator);

    uint64_t currentIndex = 0;

    for (selected = nexthops.begin(); selected != nexthops.end() && currentIndex != randomIndex;
         ++selected, ++currentIndex) {
    }
  } while (!canForwardToNextHop(pitEntry, *selected));

  this->sendInterest(pitEntry, selected->getFace());
}

} // namespace fw
} // namespace nfd
