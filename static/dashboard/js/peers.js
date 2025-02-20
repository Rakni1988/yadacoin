async function loadPeersData() {
    try {
        const response = await fetch('/get-peers');
        const peerStatus = await response.json();

        // Inbound Peers Table
        const inboundTable = document.getElementById('inbound-peers');
        inboundTable.innerHTML = `
            <tr>
                <th>Host</th>
                <th>Port</th>
                <th>Type</th>
                <th>Version</th>
                <th>Connected</th>
            </tr>
        `;

        peerStatus.inbound_peers.forEach(peer => {
            inboundTable.innerHTML += `
                <tr>
                    <td>${peer.host}</td>
                    <td>${peer.port}</td>
                    <td>${peer.peer_type || "Unknown"}</td>
                    <td>${peer.node_version?.join('.') || "Unknown"}</td>
                    <td>${peer.connection_duration || "N/A"}</td>
                </tr>
            `;
        });

        // Outbound Peers Table
        const outboundTable = document.getElementById('outbound-peers');
        outboundTable.innerHTML = `
            <tr>
                <th>Host</th>
                <th>Port</th>
                <th>Type</th>
                <th>Version</th>
                <th>Connected</th>
            </tr>
        `;

        peerStatus.outbound_peers.forEach(peer => {
            outboundTable.innerHTML += `
                <tr>
                    <td>${peer.host}</td>
                    <td>${peer.port}</td>
                    <td>${peer.peer_type || "Unknown"}</td>
                    <td>${peer.node_version?.join('.') || "Unknown"}</td>
                    <td>${peer.connection_duration || "N/A"}</td>
                </tr>
            `;
        });

    } catch (error) {
        console.error("Error loading peers data:", error);
    }
}
