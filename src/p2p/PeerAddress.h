// Copyright (c) 2018-2024, The DeroGold Developers
//
// Please see the included LICENSE file for more information.

#pragma once

#include <cstdint>
#include <string>

/* Strict parsing of the addresses typed at the daemon console.

   Common::parseIpAddressAndPort is sscanf-based and lets "1.2.3.4:80abc" and
   negative numbers through, which is acceptable for a config file the operator
   wrote once but not for a command that makes the node dial whatever it is
   given. Header only, with nothing but the standard library, so it can be
   tested on its own. */
namespace CryptoNote
{
    /* Dotted quad only, four octets of one to three digits each, nothing
       before or after. The result is in the layout NetworkAddress::ip uses: the
       first octet in the low byte. */
    inline bool parseIpv4Literal(const std::string &text, uint32_t &ip)
    {
        uint32_t value = 0;
        size_t pos = 0;

        for (size_t octetIndex = 0; octetIndex < 4; ++octetIndex)
        {
            size_t digits = 0;
            uint32_t octet = 0;

            while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9')
            {
                octet = octet * 10 + static_cast<uint32_t>(text[pos] - '0');

                if (++digits > 3 || octet > 255)
                {
                    return false;
                }

                ++pos;
            }

            if (digits == 0)
            {
                return false;
            }

            value |= octet << (8 * octetIndex);

            if (octetIndex < 3)
            {
                if (pos >= text.size() || text[pos] != '.')
                {
                    return false;
                }

                ++pos;
            }
        }

        if (pos != text.size())
        {
            return false;
        }

        ip = value;
        return true;
    }

    /* A TCP port: digits only, 1 to 65535. */
    inline bool parsePortNumber(const std::string &text, uint32_t &port)
    {
        if (text.empty() || text.size() > 5)
        {
            return false;
        }

        uint32_t value = 0;

        for (const char c : text)
        {
            if (c < '0' || c > '9')
            {
                return false;
            }

            value = value * 10 + static_cast<uint32_t>(c - '0');
        }

        if (value == 0 || value > 65535)
        {
            return false;
        }

        port = value;
        return true;
    }

    /* Splits "host:port" at the last colon. The host is not validated here: it
       is a dotted quad or a name the caller resolves. */
    inline bool splitHostAndPort(const std::string &text, std::string &host, uint32_t &port)
    {
        const size_t colon = text.rfind(':');

        if (colon == std::string::npos || colon == 0)
        {
            return false;
        }

        uint32_t parsedPort = 0;

        if (!parsePortNumber(text.substr(colon + 1), parsedPort))
        {
            return false;
        }

        host = text.substr(0, colon);
        port = parsedPort;
        return true;
    }
} // namespace CryptoNote
