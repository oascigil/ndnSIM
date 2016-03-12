/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/**
 * Copyright (c) 2014-2015,  Regents of the University of California,
 *                           Arizona Board of Regents,
 *                           Colorado State University,
 *                           University Pierre & Marie Curie, Sorbonne University,
 *                           Washington University in St. Louis,
 *                           Beijing Institute of Technology,
 *                           The University of Memphis.
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

#include "forwarder.hpp"
#include "core/logger.hpp"
#include "core/random.hpp"
#include "strategy.hpp"
#include "face/null-face.hpp"

#include "utils/ndn-ns3-packet-tag.hpp"

#include <boost/random/uniform_int_distribution.hpp>

// Onur: To read hop count
//#include "utils/ndn-ns3-packet-tag.hpp"
#include "utils/ndn-fw-hop-count-tag.hpp"
#include "ns3/packet.h"

namespace nfd {

NFD_LOG_INIT("Forwarder");

using fw::Strategy;

const Name Forwarder::LOCALHOST_NAME("ndn:/localhost");

Forwarder::Forwarder()
  : m_faceTable(*this)
  , m_fib(m_nameTree)
  , m_pit(m_nameTree)
  , m_sit(m_nameTree_sit, 11000)
  , m_measurements(m_nameTree)
  , m_strategyChoice(m_nameTree, fw::makeDefaultStrategy(*this))
  , m_csFace(make_shared<NullFace>(FaceUri("contentstore://")))
{
  fw::installStrategies(*this);
  getFaceTable().addReserved(m_csFace, FACEID_CONTENT_STORE);
}

Forwarder::~Forwarder()
{

}

void
Forwarder::onIncomingInterest(Face& inFace, const Interest& interest)
{
  // receive Interest
  NFD_LOG_DEBUG("onIncomingInterest face=" << inFace.getId() <<
                " interest=" << interest.getName() << " FloodFlag " << interest.getFloodFlag()<<" Destination Flag "<<interest.getDestinationFlag());
/*
  if(interest.getName().size() <= 3)
  { 
    NFD_LOG_INFO("onIncomingInterest face=" << inFace.getId() <<
                " interest=" << interest.getName() << " FloodFlag " << interest.getFloodFlag()<<" Destination Flag "<<interest.getDestinationFlag());
  }
*/
  const_cast<Interest&>(interest).setIncomingFaceId(inFace.getId());
  ++m_counters.getNInInterests();

  // /localhost scope control
  bool isViolatingLocalhost = !inFace.isLocal() &&
                              LOCALHOST_NAME.isPrefixOf(interest.getName());
  if (isViolatingLocalhost) {
    NFD_LOG_DEBUG("onIncomingInterest face=" << inFace.getId() <<
                  " interest=" << interest.getName() << " violates /localhost");
    // (drop)
    return;
  }

  // PIT insert
  shared_ptr<pit::Entry> pitEntry = m_pit.insert(interest).first;

  // detect duplicate Nonce
  int dnw = pitEntry->findNonce(interest.getNonce(), inFace);
  bool hasDuplicateNonce = (dnw != pit::DUPLICATE_NONCE_NONE) ||
                           m_deadNonceList.has(interest.getName(), interest.getNonce());
  if (hasDuplicateNonce) {
    // goto Interest loop pipeline
    this->onInterestLoop(inFace, interest, pitEntry);
    return;
  }

  // cancel unsatisfy & straggler timer
  this->cancelUnsatisfyAndStragglerTimer(pitEntry);

  // is pending?
  const pit::InRecordCollection& inRecords = pitEntry->getInRecords();
  bool isPending = inRecords.begin() != inRecords.end();
  if (!isPending) {
    if (m_csFromNdnSim == nullptr) {
	   
      m_cs.find(interest,
                bind(&Forwarder::onContentStoreHit, this, ref(inFace), pitEntry, _1, _2),
                bind(&Forwarder::onContentStoreMiss, this, ref(inFace), pitEntry, _1));
    }
    else {
      shared_ptr<Data> match = m_csFromNdnSim->Lookup(interest.shared_from_this());
      if (match != nullptr) {
        this->onContentStoreHit(inFace, pitEntry, interest, *match);
      }
      else {
        this->onContentStoreMiss(inFace, pitEntry, interest);
      }
    }
  }
  else {
    this->onContentStoreMiss(inFace, pitEntry, interest);
  }
}

void
Forwarder::onContentStoreMiss(const Face& inFace,
                              shared_ptr<pit::Entry> pitEntry,
                              const Interest& interest)
{
  NFD_LOG_DEBUG("onContentStoreMiss interest=" << interest.getName());

  shared_ptr<Face> face = const_pointer_cast<Face>(inFace.shared_from_this());
  // insert InRecord
  pitEntry->insertOrUpdateInRecord(face, interest);

  // set PIT unsatisfy timer
  this->setUnsatisfyTimer(pitEntry);

// FIB lookup
   shared_ptr<fib::Entry> fibEntry = m_fib.findLongestPrefixMatch(*pitEntry); //NDN
	    
 /* Flooding:
  shared_ptr<fib::Entry> fibEntry = m_fib.findLongestPrefixMatch(*pitEntry); //NDN
  if(m_fib.isEmpty(fibEntry))
  {
    unsigned int hopCount = 0;
    auto ns3PacketTag = interest.getTag<ns3::ndn::Ns3PacketTag>();
    if (ns3PacketTag != nullptr) // e.g., packet came from local node's cache
    {
      ns3::ndn::FwHopCountTag hopCountTag;
      if (ns3PacketTag->getPacket()->PeekPacketTag(hopCountTag)) {
        hopCount = hopCountTag.Get();
        NFD_LOG_DEBUG("Interest Hop count: " << hopCount);
      }
    }
    if(hopCount < interest.getFloodFlag())
    {
      NFD_LOG_DEBUG("PIT Entry Flood Flag is set: " << interest.getFloodFlag());
	   (*pitEntry).setFloodFlag(); 
    }
    else
      NFD_LOG_DEBUG("Won't Flood, Scope is complete: " << interest.getName());
  }
 */ // End of Flooding


/*
  shared_ptr<fib::Entry> fibEntry;
  shared_ptr<fib::Entry> sitEntry;
  // FIB lookup
  if(0 == interest.getDestinationFlag())
  {
    if(0 == interest.getFloodFlag())
	 {
      fibEntry = m_fib.findLongestPrefixMatch(*pitEntry);
      if(m_fib.isEmpty(fibEntry)) { //check SIT table
        sitEntry = m_sit.findExactMatch((*pitEntry).getName());         
	     if(static_cast<bool>(sitEntry))
		  {
          NFD_LOG_DEBUG("SIT table lookup succeeded for: " << interest.getName());
	       (*pitEntry).setDestinationFlag(); 
		    fibEntry = sitEntry;
		  }
      }
		else
        NFD_LOG_DEBUG("FIB table lookup succeeded for: " << interest.getName());

    }
	 else //flood flag is set
	 {
       NFD_LOG_DEBUG("Flood Flag Set in the Interest for: " << interest.getName());
	    unsigned int hopCount = 0;
       auto ns3PacketTag = interest.getTag<ns3::ndn::Ns3PacketTag>();
       if (ns3PacketTag != nullptr) // e.g., packet came from local node's cache
		 {
         ns3::ndn::FwHopCountTag hopCountTag;
         if (ns3PacketTag->getPacket()->PeekPacketTag(hopCountTag)) {
           hopCount = hopCountTag.Get();
           NFD_LOG_DEBUG("Interest Hop count: " << hopCount);
			}
	    }

	    //flood if no SIT match
       fibEntry = m_fib.findLongestPrefixMatch(*pitEntry);
      if(m_fib.isEmpty(fibEntry)) 
		{ //check SIT table
        sitEntry = m_sit.findExactMatch((*pitEntry).getName());         
	     if(static_cast<bool>(sitEntry))
		  {
          NFD_LOG_DEBUG("SIT table lookup succeeded for: " << interest.getName());
	       (*pitEntry).setDestinationFlag(); 
		    fibEntry = sitEntry;
		  }
        else
		  {
		    if(hopCount < interest.getFloodFlag())
			 {
            NFD_LOG_DEBUG("PIT Entry Flood Flag is set: " << interest.getFloodFlag());
	         (*pitEntry).setFloodFlag(); 
			 }
			 else
            NFD_LOG_DEBUG("Won't Flood, Scope is complete: " << interest.getName());
		  }
	   }
		else
          NFD_LOG_DEBUG("FIB table lookup succeeded for: " << interest.getName());
	 }
  }
  else //destination flag == 1
  {
    NFD_LOG_DEBUG("Received a packet with DF set to 1: " << interest.getName());
    sitEntry = m_sit.findExactMatch((*pitEntry).getName());         
	 if(!static_cast<bool>(sitEntry))
	 {
      NFD_LOG_DEBUG("onContentStoreMiss: ERROR, no match in SIT table");
		//TODO send a NACK???
      fibEntry = m_fib.findLongestPrefixMatch(*pitEntry);
    }
	 else
	 {
	   (*pitEntry).setDestinationFlag(); 
      NFD_LOG_DEBUG("Found an entry for: "<<(*pitEntry).getName()<<" in the SIT table. destination flag is 1");
	   fibEntry = sitEntry;
	 }
  }
*/
  // dispatch to strategy
  this->dispatchToStrategy(pitEntry, bind(&Strategy::afterReceiveInterest, _1,
                                          cref(inFace), cref(interest), fibEntry, pitEntry));
}

void
Forwarder::onContentStoreHit(const Face& inFace,
                             shared_ptr<pit::Entry> pitEntry,
                             const Interest& interest,
                             const Data& data)
{
  NFD_LOG_DEBUG("onContentStoreHit interest=" << interest.getName());
  if(interest.getName().size() == 2)
  {
    NFD_LOG_INFO("Content Store Hit for "<<interest.getName().at(-1).toSequenceNumber());
  }

  beforeSatisfyInterest(*pitEntry, *m_csFace, data);
  this->dispatchToStrategy(pitEntry, bind(&Strategy::beforeSatisfyInterest, _1,
                                          pitEntry, cref(*m_csFace), cref(data)));

  const_pointer_cast<Data>(data.shared_from_this())->setIncomingFaceId(FACEID_CONTENT_STORE);
  // XXX should we lookup PIT for other Interests that also match csMatch?

  // set PIT straggler timer
  this->setStragglerTimer(pitEntry, true, data.getFreshnessPeriod());

  // goto outgoing Data pipeline
  this->onOutgoingData(data, *const_pointer_cast<Face>(inFace.shared_from_this()));
}

void
Forwarder::onInterestLoop(Face& inFace, const Interest& interest,
                          shared_ptr<pit::Entry> pitEntry)
{
  NFD_LOG_DEBUG("onInterestLoop face=" << inFace.getId() <<
                " interest=" << interest.getName());

  // (drop)
}

/** \brief compare two InRecords for picking outgoing Interest
 *  \return true if b is preferred over a
 *
 *  This function should be passed to std::max_element over InRecordCollection.
 *  The outgoing Interest picked is the last incoming Interest
 *  that does not come from outFace.
 *  If all InRecords come from outFace, it's fine to pick that. This happens when
 *  there's only one InRecord that comes from outFace. The legit use is for
 *  vehicular network; otherwise, strategy shouldn't send to the sole inFace.
 */
static inline bool
compare_pickInterest(const pit::InRecord& a, const pit::InRecord& b, const Face* outFace)
{
  bool isOutFaceA = a.getFace().get() == outFace;
  bool isOutFaceB = b.getFace().get() == outFace;

  if (!isOutFaceA && isOutFaceB) {
    return false;
  }
  if (isOutFaceA && !isOutFaceB) {
    return true;
  }

  return a.getLastRenewed() > b.getLastRenewed();
}

void
Forwarder::onOutgoingInterest(shared_ptr<pit::Entry> pitEntry, Face& outFace,
                              bool wantNewNonce)
{
  if (outFace.getId() == INVALID_FACEID) {
    NFD_LOG_WARN("onOutgoingInterest face=invalid interest=" << pitEntry->getName());
    return;
  }
  NFD_LOG_DEBUG("onOutgoingInterest face=" << outFace.getId() <<
                " interest=" << pitEntry->getName());

  // scope control
  if (pitEntry->violatesScope(outFace)) {
    NFD_LOG_DEBUG("onOutgoingInterest face=" << outFace.getId() <<
                  " interest=" << pitEntry->getName() << " violates scope");
    return;
  }

  // pick Interest
  const pit::InRecordCollection& inRecords = pitEntry->getInRecords();
  pit::InRecordCollection::const_iterator pickedInRecord = std::max_element(
    inRecords.begin(), inRecords.end(), bind(&compare_pickInterest, _1, _2, &outFace));
  BOOST_ASSERT(pickedInRecord != inRecords.end());
  shared_ptr<Interest> interest = const_pointer_cast<Interest>(
    pickedInRecord->getInterest().shared_from_this());

  if (wantNewNonce) {
    interest = make_shared<Interest>(*interest);
    static boost::random::uniform_int_distribution<uint32_t> dist;
    interest->setNonce(dist(getGlobalRng()));
  }

  if(pitEntry->getDestinationFlag())
  { //set the destination flag in the Interest packet
    interest->setDestinationFlag(1);
	 interest->setFloodFlag(0);
	 //std::cout << "Setting the destination Flag in the out-going interest packet: "<< pitEntry->getName() << "\n"; 
  }
  
  if(interest->getName().size() == 2)
  { 
    NFD_LOG_DEBUG("onOutgoingInterest"<< 
                " name=" << interest->getName() << " FloodFlag " << interest->getFloodFlag()<<" Destination Flag "<<interest->getDestinationFlag());
    //NFD_LOG_INFO("onOutgoingInterest " << interest->getName().at(-1).toSequenceNumber());
  }

  // insert OutRecord
  pitEntry->insertOrUpdateOutRecord(outFace.shared_from_this(), *interest);

  // send Interest
  outFace.sendInterest(*interest);
  ++m_counters.getNOutInterests();
}

void
Forwarder::onInterestReject(shared_ptr<pit::Entry> pitEntry)
{
  if (pitEntry->hasUnexpiredOutRecords()) {
    NFD_LOG_ERROR("onInterestReject interest=" << pitEntry->getName() <<
                  " cannot reject forwarded Interest");
    return;
  }
  NFD_LOG_DEBUG("onInterestReject interest=" << pitEntry->getName());

  // cancel unsatisfy & straggler timer
  this->cancelUnsatisfyAndStragglerTimer(pitEntry);

  // set PIT straggler timer
  this->setStragglerTimer(pitEntry, false);
}

void
Forwarder::onInterestUnsatisfied(shared_ptr<pit::Entry> pitEntry)
{
  NFD_LOG_DEBUG("onInterestUnsatisfied interest=" << pitEntry->getName());

  // invoke PIT unsatisfied callback
  beforeExpirePendingInterest(*pitEntry);
  this->dispatchToStrategy(pitEntry, bind(&Strategy::beforeExpirePendingInterest, _1,
                                          pitEntry));

  // goto Interest Finalize pipeline
  this->onInterestFinalize(pitEntry, false);
}

void
Forwarder::onInterestFinalize(shared_ptr<pit::Entry> pitEntry, bool isSatisfied,
                              const time::milliseconds& dataFreshnessPeriod)
{
  NFD_LOG_DEBUG("onInterestFinalize interest=" << pitEntry->getName() <<
                (isSatisfied ? " satisfied" : " unsatisfied"));

  // Dead Nonce List insert if necessary
  this->insertDeadNonceList(*pitEntry, isSatisfied, dataFreshnessPeriod, 0);

  // PIT delete
  this->cancelUnsatisfyAndStragglerTimer(pitEntry);
  m_pit.erase(pitEntry);
}

void
Forwarder::onIncomingData(Face& inFace, const Data& data)
{
  // receive Data
  /*
  if(data.getName().size() <= 3)
  { 
    NFD_LOG_INFO("onIncomingData face=" << inFace.getId());
  }
  */
  NFD_LOG_DEBUG("onIncomingData face=" << inFace.getId() << " data=" << data.getName());
  const_cast<Data&>(data).setIncomingFaceId(inFace.getId());
  ++m_counters.getNInDatas();

  // /localhost scope control
  bool isViolatingLocalhost = !inFace.isLocal() &&
                              LOCALHOST_NAME.isPrefixOf(data.getName());
  if (isViolatingLocalhost) {
    NFD_LOG_DEBUG("onIncomingData face=" << inFace.getId() <<
                  " data=" << data.getName() << " violates /localhost");
    // (drop)
    return;
  }

  // PIT match
  pit::DataMatchResult pitMatches = m_pit.findAllDataMatches(data);
  if (pitMatches.begin() == pitMatches.end()) {
    // goto Data unsolicited pipeline
    this->onDataUnsolicited(inFace, data);
    return;
  }

  // Remove Ptr<Packet> from the Data before inserting into cache, serving two purposes
  // - reduce amount of memory used by cached entries
  // - remove all tags that (e.g., hop count tag) that could have been associated with Ptr<Packet>
  //
  // Copying of Data is relatively cheap operation, as it copies (mostly) a collection of Blocks
  // pointing to the same underlying memory buffer.
  shared_ptr<Data> dataCopyWithoutPacket = make_shared<Data>(data);
  dataCopyWithoutPacket->removeTag<ns3::ndn::Ns3PacketTag>();

  // CS insert
  //if(data.getName().size() <= 3) 
  //{ //don't cache mgmt data (e.g. FIB Manager)
    if (m_csFromNdnSim == nullptr)
      m_cs.insert(*dataCopyWithoutPacket);
    else
      m_csFromNdnSim->Add(dataCopyWithoutPacket);
  //}
  std::set<shared_ptr<Face> > pendingDownstreams;
  // foreach PitEntry
  for (const shared_ptr<pit::Entry>& pitEntry : pitMatches) {
    NFD_LOG_DEBUG("onIncomingData matching=" << pitEntry->getName());

    // cancel unsatisfy & straggler timer
    this->cancelUnsatisfyAndStragglerTimer(pitEntry);

    // remember pending downstreams
    const pit::InRecordCollection& inRecords = pitEntry->getInRecords();
    for (pit::InRecordCollection::const_iterator it = inRecords.begin();
                                                 it != inRecords.end(); ++it) {
      if (it->getExpiry() > time::steady_clock::now()) {
        pendingDownstreams.insert(it->getFace());
      }
    }

    // invoke PIT satisfy callback
    beforeSatisfyInterest(*pitEntry, inFace, data);
    this->dispatchToStrategy(pitEntry, bind(&Strategy::beforeSatisfyInterest, _1,
                                            pitEntry, cref(inFace), cref(data)));

    // Dead Nonce List insert if necessary (for OutRecord of inFace)
    this->insertDeadNonceList(*pitEntry, true, data.getFreshnessPeriod(), &inFace);

    // mark PIT satisfied
    pitEntry->deleteInRecords();
    pitEntry->deleteOutRecord(inFace);

    // set PIT straggler timer
    this->setStragglerTimer(pitEntry, true, data.getFreshnessPeriod());
  }

  // insert SIT enrty: first lookup the name
  shared_ptr<fib::Entry> sitEntry = m_sit.findExactMatch(data.getName());
  if(!static_cast<bool>(sitEntry))
  {
    //std::cout<<"Inserting sitEntry: "<<data.getName()<<"\n";
    sitEntry = m_sit.insert(data.getName()).first;
  }

  // foreach pending downstream
  for (std::set<shared_ptr<Face> >::iterator it = pendingDownstreams.begin();
      it != pendingDownstreams.end(); ++it) {
    shared_ptr<Face> pendingDownstream = getFace((*it)->getId());
    if (pendingDownstream.get() == &inFace) {
      continue;
    }
    //insert SIT entry
	 //if(static_cast<bool> (sitEntry) )
      sitEntry->addNextHop(pendingDownstream, 0);

    // goto outgoing Data pipeline
    this->onOutgoingData(data, *pendingDownstream);
  }
}

void
Forwarder::onDataUnsolicited(Face& inFace, const Data& data)
{
  // accept to cache?
  bool acceptToCache = inFace.isLocal();
  if (acceptToCache) {
    // CS insert
    if (m_csFromNdnSim == nullptr)
	 {
	   if(data.getName().size() <= 3) 
		{ //This is a hack to prevent mgmt data (e.g. FIB Manager) from being cached
        m_cs.insert(data, true);
		}
	 }
    else
	 {
	   if(data.getName().size() <= 3) 
		{ //This is a hack to prevent mgmt data (e.g. FIB Manager) from being cached
        m_csFromNdnSim->Add(data.shared_from_this());
		}
	 }
  }

  NFD_LOG_DEBUG("onDataUnsolicited face=" << inFace.getId() <<
                " data=" << data.getName() <<
                (acceptToCache ? " cached" : " not cached"));
}

void
Forwarder::onOutgoingData(const Data& data, Face& outFace)
{
  if (outFace.getId() == INVALID_FACEID) {
    NFD_LOG_WARN("onOutgoingData face=invalid data=" << data.getName());
    return;
  }
  NFD_LOG_DEBUG("onOutgoingData face=" << outFace.getId() << " data=" << data.getName());
  if(data.getName().size() == 2)
  { 
    NFD_LOG_INFO("onOutgoingData " << data.getName().at(-1).toSequenceNumber());
  }

  // /localhost scope control
  bool isViolatingLocalhost = !outFace.isLocal() &&
                              LOCALHOST_NAME.isPrefixOf(data.getName());
  if (isViolatingLocalhost) {
    NFD_LOG_DEBUG("onOutgoingData face=" << outFace.getId() <<
                  " data=" << data.getName() << " violates /localhost");
    // (drop)
    return;
  }

  // TODO traffic manager

  // send Data
  outFace.sendData(data);
  ++m_counters.getNOutDatas();
}

static inline bool
compare_InRecord_expiry(const pit::InRecord& a, const pit::InRecord& b)
{
  return a.getExpiry() < b.getExpiry();
}

void
Forwarder::setUnsatisfyTimer(shared_ptr<pit::Entry> pitEntry)
{
  const pit::InRecordCollection& inRecords = pitEntry->getInRecords();
  pit::InRecordCollection::const_iterator lastExpiring =
    std::max_element(inRecords.begin(), inRecords.end(),
    &compare_InRecord_expiry);

  time::steady_clock::TimePoint lastExpiry = lastExpiring->getExpiry();
  time::nanoseconds lastExpiryFromNow = lastExpiry  - time::steady_clock::now();
  if (lastExpiryFromNow <= time::seconds(0)) {
    // TODO all InRecords are already expired; will this happen?
  }

  scheduler::cancel(pitEntry->m_unsatisfyTimer);
  pitEntry->m_unsatisfyTimer = scheduler::schedule(lastExpiryFromNow,
    bind(&Forwarder::onInterestUnsatisfied, this, pitEntry));
}

void
Forwarder::setStragglerTimer(shared_ptr<pit::Entry> pitEntry, bool isSatisfied,
                             const time::milliseconds& dataFreshnessPeriod)
{
  time::nanoseconds stragglerTime = time::milliseconds(100);

  scheduler::cancel(pitEntry->m_stragglerTimer);
  pitEntry->m_stragglerTimer = scheduler::schedule(stragglerTime,
    bind(&Forwarder::onInterestFinalize, this, pitEntry, isSatisfied, dataFreshnessPeriod));
}

void
Forwarder::cancelUnsatisfyAndStragglerTimer(shared_ptr<pit::Entry> pitEntry)
{
  scheduler::cancel(pitEntry->m_unsatisfyTimer);
  scheduler::cancel(pitEntry->m_stragglerTimer);
}

static inline void
insertNonceToDnl(DeadNonceList& dnl, const pit::Entry& pitEntry,
                 const pit::OutRecord& outRecord)
{
  dnl.add(pitEntry.getName(), outRecord.getLastNonce());
}

void
Forwarder::insertDeadNonceList(pit::Entry& pitEntry, bool isSatisfied,
                               const time::milliseconds& dataFreshnessPeriod,
                               Face* upstream)
{
  // need Dead Nonce List insert?
  bool needDnl = false;
  if (isSatisfied) {
    bool hasFreshnessPeriod = dataFreshnessPeriod >= time::milliseconds::zero();
    // Data never becomes stale if it doesn't have FreshnessPeriod field
    needDnl = static_cast<bool>(pitEntry.getInterest().getMustBeFresh()) &&
              (hasFreshnessPeriod && dataFreshnessPeriod < m_deadNonceList.getLifetime());
  }
  else {
    needDnl = true;
  }

  if (!needDnl) {
    return;
  }

  // Dead Nonce List insert
  if (upstream == 0) {
    // insert all outgoing Nonces
    const pit::OutRecordCollection& outRecords = pitEntry.getOutRecords();
    std::for_each(outRecords.begin(), outRecords.end(),
                  bind(&insertNonceToDnl, ref(m_deadNonceList), cref(pitEntry), _1));
  }
  else {
    // insert outgoing Nonce of a specific face
    pit::OutRecordCollection::const_iterator outRecord = pitEntry.getOutRecord(*upstream);
    if (outRecord != pitEntry.getOutRecords().end()) {
      m_deadNonceList.add(pitEntry.getName(), outRecord->getLastNonce());
    }
  }
}

} // namespace nfd
