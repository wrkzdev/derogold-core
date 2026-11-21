// Copyright (c) 2018-2024, The DeroGold Developers
// Copyright (c) 2012-2017, The CryptoNote developers, The Bytecoin developers
// Copyright (c) 2018-2019, The TurtleCoin Developers
// Copyright (c) 2019, The CyprusCoin Developers
//
// Please see the included LICENSE file for more information.

#pragma once

#include "ConnectionContext.h"
#include "LevinProtocol.h"
#include "NetNodeCommon.h"
#include "NetNodeConfig.h"
#include "P2pProtocolDefinitions.h"
#include "PeerListManager.h"
#include "cryptonoteprotocol/CryptoNoteProtocolHandler.h"
#include "logging/LoggerRef.h"
#include "p2p/OnceInInterval.h"

#include <Uuid.h>
#include <atomic>
#include <chrono>
#include <config/CryptoNoteConfig.h>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <thread>
#include <system/Context.h>
#include <system/ContextGroup.h>
#include <system/Dispatcher.h>
#include <system/Event.h>
#include <system/TcpConnection.h>
#include <system/TcpListener.h>
#include <system/Timer.h>
#include <unordered_map>
#include <utility>

namespace System
{
    class TcpConnection;
}

namespace CryptoNote
{
    class LevinProtocol;

    class ISerializer;

    struct P2pMessage
    {
        enum Type
        {
            COMMAND,
            REPLY,
            NOTIFY
        };

        P2pMessage(Type type, uint32_t command, BinaryArray buffer, int32_t returnCode = 0):
            type(type),
            command(command),
            buffer(std::move(buffer)),
            returnCode(returnCode)
        {
        }

        P2pMessage(P2pMessage &&msg):
            type(msg.type),
            command(msg.command),
            buffer(std::move(msg.buffer)),
            returnCode(msg.returnCode)
        {
        }

        size_t size()
        {
            return buffer.size();
        }

        Type type;

        uint32_t command;

        /* Not const: a const member silently turned every move of this message
           into a copy, so each queued block response was duplicated in full on
           its way to the send queue. */
        BinaryArray buffer;

        int32_t returnCode;
    };

    struct P2pConnectionContext : public CryptoNoteConnectionContext
    {
      public:
        using Clock = std::chrono::steady_clock;
        using TimePoint = Clock::time_point;

        System::Context<void> *context;

        uint64_t peerId;

        System::TcpConnection connection;

        P2pConnectionContext(
            System::Dispatcher &dispatcher,
            std::shared_ptr<Logging::ILogger> log,
            System::TcpConnection &&conn):
            context(nullptr),
            peerId(0),
            connection(std::move(conn)),
            logger(log, "node_server"),
            queueEvent(dispatcher),
            stopped(false)
        {
        }

        P2pConnectionContext(P2pConnectionContext &&ctx):
            CryptoNoteConnectionContext(std::move(ctx)),
            context(ctx.context),
            peerId(ctx.peerId),
            connection(std::move(ctx.connection)),
            logger(ctx.logger.getLogger(), "node_server"),
            queueEvent(std::move(ctx.queueEvent)),
            stopped(ctx.stopped)
        {
        }

        bool pushMessage(P2pMessage &&msg);

        std::vector<P2pMessage> popBuffer();

        void interrupt();

        uint64_t writeDuration(TimePoint now) const;

        /* Milliseconds since we last read anything from this peer. */
        uint64_t readIdleDuration(TimePoint now) const;

        /* Called by the connection handler after every command it reads. */
        void markRead();

      private:
        Logging::LoggerRef logger;

        TimePoint writeOperationStartTime;

        /* When we last read a command from this peer. Only the write side was
           tracked, so a peer that accepted our request and then went quiet was
           never noticed: it sat in the synchronizing state holding one of the
           few outgoing slots for the lifetime of the process. */
        TimePoint lastReadTime = Clock::now();

        System::Event queueEvent;

        std::vector<P2pMessage> writeQueue;

        size_t writeQueueSize = 0;

        bool stopped;
    };

    class NodeServer : public IP2pEndpoint
    {
      public:
        NodeServer(
            System::Dispatcher &dispatcher,
            CryptoNote::CryptoNoteProtocolHandler &payload_handler,
            std::shared_ptr<Logging::ILogger> log);

        /* Not defaulted: the seed resolver thread has to be joined before the
           members it captures go away. */
        ~NodeServer() override;

        bool run();

        bool init(const NetNodeConfig &config);

        bool deinit();

        bool sendStopSignal();

        uint32_t get_this_peer_port() const
        {
            return m_listeningPort;
        }

        CryptoNote::CryptoNoteProtocolHandler &get_payload_object();

        void serialize(ISerializer &s);

        // debug functions
        bool log_peerlist();

        bool log_connections();

        uint64_t get_connections_count() override;

        size_t get_outgoing_connections_count();

        size_t get_incoming_connections_count();

        PeerlistManager &getPeerlistManager()
        {
            return m_peerlist;
        }

        /* Operator commands, for the daemon console. Every one of them is safe
           to call from any thread: the work is handed to the dispatcher thread
           that owns the connection and peer lists, and the caller waits for the
           answer, which is text meant to be shown as it is. A call gives up
           waiting after a while rather than hang a console on a node that is
           stopping; the work it asked for may still finish afterwards.

           Addresses are NetworkAddress values: the first octet in the low byte
           of ip, as everywhere else in the node. Hostnames must already have
           been resolved, because a lookup blocks and this thread is not the
           place for that. */

        /* Peer counts, bans, cool-downs, the white list and, when asked for, the
           gray list. */
        std::string peerListReport(bool includeGray);

        /* The connection table that print_cn shows. */
        std::string connectionsReport();

        /* Dials the peer now, as a full connection that can be synced from, past
           the out-peers target and any cool-down it is under. With keep the node
           also redials it whenever the connection drops, until the next
           restart. */
        std::string connectToPeer(const NetworkAddress &address, bool keep);

        /* Puts an address on the gray list without dialling it. */
        std::string addPeer(const NetworkAddress &address);

        /* Closes connections to the host. A port of 0 matches every port, which
           is what an incoming connection needs because its port is not one the
           peer listens on. Also ends any keep for it. */
        std::string disconnectPeer(uint32_t ip, uint32_t port);

        /* Takes the address off both lists, out of the priority list and out of
           the cool-down map. Does not close a connection that is open. */
        std::string forgetPeer(const NetworkAddress &address);

        /* Asks the seed nodes for a fresh peer list right now instead of waiting
           out the retry interval, and looks the seed hostnames up again. */
        std::string reseed();

        /* Handshakes with one peer, merges the peer list it sends and closes
           the connection. */
        std::string harvestPeers(const NetworkAddress &address);

        /* Forgets every address the node is currently passing over. */
        std::string clearFailedPeers();

        std::string banReport();

        std::string banHost(uint32_t ip, uint64_t seconds);

        std::string unbanHost(uint32_t ip);

      private:
        /* Runs job on the dispatcher thread and returns what it returns. */
        std::string runOnDispatcher(std::function<std::string()> job, std::chrono::seconds timeout);

        bool is_banned(uint32_t ip);

        /* Why a console command may not dial this host, or an empty string if
           it may. */
        std::string reason_to_refuse(uint32_t ip);

        /* Closes every connection to the host; port 0 means any port. Returns
           how many it was asked to close. */
        size_t close_connections_to(uint32_t ip, uint32_t port);

        void remove_priority_peer(uint32_t ip, uint32_t port);

        int handleCommand(
            const LevinProtocol::Command &cmd,
            BinaryArray &buff_out,
            P2pConnectionContext &context,
            bool &handled);

        //----------------- commands handlers ----------------------------------------------
        int handle_handshake(
            int command,
            COMMAND_HANDSHAKE::request &arg,
            COMMAND_HANDSHAKE::response &rsp,
            P2pConnectionContext &context);

        int handle_timed_sync(
            int command,
            COMMAND_TIMED_SYNC::request &arg,
            COMMAND_TIMED_SYNC::response &rsp,
            P2pConnectionContext &context);

        int handle_ping(
            int command,
            COMMAND_PING::request &arg,
            COMMAND_PING::response &rsp,
            P2pConnectionContext &context);

#ifdef ALLOW_DEBUG_COMMANDS
        int handle_get_stat_info(
            int command,
            COMMAND_REQUEST_STAT_INFO::request &arg,
            COMMAND_REQUEST_STAT_INFO::response &rsp,
            P2pConnectionContext &context);
        int handle_get_network_state(
            int command,
            COMMAND_REQUEST_NETWORK_STATE::request &arg,
            COMMAND_REQUEST_NETWORK_STATE::response &rsp,
            P2pConnectionContext &context);
        int handle_get_peer_id(
            int command,
            COMMAND_REQUEST_PEER_ID::request &arg,
            COMMAND_REQUEST_PEER_ID::response &rsp,
            P2pConnectionContext &context);
        bool check_trust(const proof_of_trust &tr);
#endif

        bool init_config();

        bool make_default_config();

        bool store_config();

        void initUpnp();

        /* failure, when given, is set to why a handshake that returned false
           failed, in words for an operator. */
        bool handshake(
            CryptoNote::LevinProtocol &proto,
            P2pConnectionContext &context,
            bool just_take_peerlist = false,
            std::string *failure = nullptr);

        bool timedSync();

        bool handleTimedSyncResponse(const BinaryArray &in, P2pConnectionContext &context);

        void forEachConnection(std::function<void(P2pConnectionContext &)> action);

        void on_connection_new(P2pConnectionContext &context);

        void on_connection_close(P2pConnectionContext &context);

        //----------------- i_p2p_endpoint -------------------------------------------------------------
        virtual void relay_notify_to_all(
            int command,
            const BinaryArray &data_buff,
            const Common::Uuid *excludeConnection) override;

        virtual bool invoke_notify_to_peer(
            int command,
            const BinaryArray &req_buff,
            const CryptoNoteConnectionContext &context) override;

        virtual void
            for_each_connection(std::function<void(CryptoNote::CryptoNoteConnectionContext &, uint64_t)> f) override;

        virtual void externalRelayNotifyToAll(
            int command,
            const BinaryArray &data_buff,
            const Common::Uuid *excludeConnection) override;

        virtual void externalRelayNotifyToList(
            int command,
            const BinaryArray &data_buff,
            const std::list<Common::Uuid> relayList) override;

        //-----------------------------------------------------------------------------------------------
        bool handleConfig(const NetNodeConfig &config);

        bool idle_worker();

        bool handle_remote_peerlist(
            const std::list<PeerlistEntry> &peerlist,
            time_t local_time,
            const CryptoNoteConnectionContext &context);

        bool get_local_node_data(basic_node_data &node_data);

        bool merge_peerlist_with_local(const std::list<PeerlistEntry> &bs);

        bool fix_time_delta(std::list<PeerlistEntry> &local_peerlist, time_t local_time, int64_t &delta);

        bool connections_maker();

        bool connect_to_seeds();

        /* Turns the seed hostnames into addresses. Runs on the resolver thread
           as well as in init(), so it touches nothing but its arguments. */
        void resolve_seed_nodes(const std::vector<std::string> &extraHosts, std::vector<NetworkAddress> &nodes);

        void start_seed_resolve();

        void collect_seed_resolve_result();

        void join_seed_resolve_thread();

        /* An address that just refused a connection is passed over for
           P2P_FAILED_PEER_FORGET_SECONDS, so a handful of dead peers cannot
           consume every connection attempt round after round. */
        bool is_addr_recently_failed(const NetworkAddress &addr);

        /* seconds is how long the address is passed over; the default is the
           time an address that would not connect is given. A shorter mark never
           shortens one that is already longer. */
        void mark_addr_failed(const NetworkAddress &addr, uint32_t seconds = CryptoNote::P2P_FAILED_PEER_FORGET_SECONDS);

        bool gray_peerlist_housekeeping();

        bool make_new_connection_from_peerlist(bool use_white_list);

        bool try_to_connect_and_handshake_with_new_peer(
            const NetworkAddress &na,
            bool just_take_peerlist = false,
            uint64_t last_seen_stamp = 0,
            bool white = true,
            std::string *failure = nullptr);

        bool is_peer_used(const PeerlistEntry &peer);

        bool is_addr_connected(const NetworkAddress &peer);

        bool try_ping(basic_node_data &node_data, P2pConnectionContext &context);

        bool make_expected_connections_count(bool white_list, size_t expected_connections);

        bool connect_to_peerlist(const std::vector<NetworkAddress> &peers);

        // debug functions
        std::string print_connections_container();

        typedef std::unordered_map<Common::Uuid, P2pConnectionContext>
            ConnectionContainer;

        typedef ConnectionContainer::iterator ConnectionIterator;

        ConnectionContainer m_connections;

        void acceptLoop();

        void connectionHandler(const Common::Uuid &connectionId, P2pConnectionContext &connection);

        void writeHandler(P2pConnectionContext &ctx);

        void onIdle();

        void timedSyncLoop();

        void timeoutLoop();

        template<typename T> void safeInterrupt(T &obj);

        struct config
        {
            network_config m_net_config;

            uint64_t m_peer_id;

            void serialize(ISerializer &s)
            {
                KV_MEMBER(m_net_config)
                KV_MEMBER(m_peer_id)
            }
        };

        config m_config;

        std::string m_config_folder;

        bool m_have_address;

        bool m_first_connection_maker_call;

        uint32_t m_listeningPort;

        uint32_t m_external_port;

        uint32_t m_ip_address;

        bool m_allow_local_ip;

        bool m_hide_my_port;

        /* --out-peers: how many outgoing connections the connection maker aims
           for. --in-peers: how many incoming ones the listener accepts before
           turning peers away. */
        uint32_t m_targetOutgoingConnections;

        uint32_t m_maxIncomingConnections;

        std::string m_p2p_state_filename;

        bool m_p2p_state_reset;

        System::Dispatcher &m_dispatcher;

        System::ContextGroup m_workingContextGroup;

        System::Event m_stopEvent;

        System::Timer m_idleTimer;

        System::Timer m_timeoutTimer;

        System::TcpListener m_listener;

        Logging::LoggerRef logger;

        std::atomic<bool> m_stop;

        CryptoNoteProtocolHandler &m_payload_handler;

        PeerlistManager m_peerlist;

        // OnceInInterval m_peer_handshake_idle_maker_interval;
        OnceInInterval m_connections_maker_interval;

        /* The connection maker runs every second; this keeps the seed rounds it
           can trigger down to one per P2P_SEED_RETRY_INTERVAL_SECONDS. */
        OnceInInterval m_seed_retry_interval;

        OnceInInterval m_gray_housekeeping_interval;

        OnceInInterval m_peerlist_store_interval;

        System::Timer m_timedSyncTimer;

        std::string m_bind_ip;

        std::string m_port;

#ifdef ALLOW_DEBUG_COMMANDS
        uint64_t m_last_stat_request_time;
#endif

        std::vector<NetworkAddress> m_priority_peers;

        std::vector<NetworkAddress> m_exclusive_peers;

        std::vector<NetworkAddress> m_seed_nodes;

        /* Seed hostnames, kept so they can be looked up again: a seed that
           moves, or a name that would not resolve when the daemon started
           before the network was up, must not be lost for good. Resolution
           happens on m_seedResolveThread because getaddrinfo blocks, and the
           dispatcher thread cannot afford to stall on a dead DNS server. */
        std::vector<std::string> m_seed_node_hosts;

        std::thread m_seedResolveThread;

        std::mutex m_seedResolveMutex;

        std::vector<NetworkAddress> m_seedResolveResult;

        bool m_seedResolveReady;

        std::atomic<bool> m_seed_resolve_in_flight;

        uint64_t m_seed_resolve_due;

        std::list<PeerlistEntry> m_command_line_peers;

        /* Address -> the time until which it is passed over. Entries expire on
           lookup, and the map is swept when it outgrows the gray list so address
           churn cannot make it grow without bound. */
        std::map<NetworkAddress, time_t> m_recentlyFailedPeers;

        /* Host -> the time its ban ends, set from the console. Held in memory
           only, so a restart lifts every ban. Keyed by IP alone: a banned host
           is refused on every port. */
        std::map<uint32_t, time_t> m_bannedHosts;

        uint64_t m_peer_livetime;

        Common::Uuid m_network_id;
    };
} // namespace CryptoNote
