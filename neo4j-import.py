import json
from py2neo import Graph, Node, Relationship, Subgraph

graph = Graph()

def main():

    with open('blocks.json') as json_file:
        data = json.load(json_file)
        for block in data['blocks']["nodes"]:
            block_payload = {
                "date": block["dateTime"],
                "snarkedLedgerHash": block["protocolState"]["blockchainState"]["snarkedLedgerHash"],
                "stagedLedgerHash": block["protocolState"]["blockchainState"]["stagedLedgerHash"],
                "totalCurrency": block["protocolState"]["consensusState"]["totalCurrency"],
                "stateHash": block["stateHash"],
                "previousStateHash": block["protocolState"]["previousStateHash"],
            }
            # Model Current Block
            currentBlock = Node("Block", **block_payload)
            if not graph.exists(currentBlock):
                graph.create(currentBlock)

            # Model Parent Relationship (If Possible)
            parentBlock = graph.find_one(label="Block", property_key="stateHash", property_value=block["protocolState"]["previousStateHash"])
            if parentBlock:
                parentRelationship = Relationship(currentBlock, "IS_PARENT", parentBlock)
                if not graph.exists(parentRelationship):
                    graph.create(parentRelationship)
            
            # Model slot and epoch
            epoch = Node("Epoch", epoch=block["protocolState"]["consensusState"]["epoch"])
            slot = Node("Slot", slot=block["protocolState"]["consensusState"]["slot"], epoch=block["protocolState"]["consensusState"]["epoch"])
            ab = Relationship(currentBlock, "IS_IN", slot)
            ac = Relationship(currentBlock, "IS_IN", epoch)
            bc = Relationship(slot, "IS_IN", epoch)
            # Commit to Graph
            graph.merge(Subgraph([epoch, slot], [ab, ac, bc]))

            # Model User Commands
            userCommands = block["transactions"]["userCommands"]
            for command in userCommands: 
                command_payload = {
                    "amount": command["amount"],
                    "txId": command["id"],
                    "isDelegation": command["isDelegation"],
                    "memo": command["memo"],
                    "nonce": command["nonce"]
                }            
                userCommand = Node("UserCommand", **command_payload)

                toWallet = Node("Wallet", publicKey=command["to"])
                fromWallet = Node("Wallet", publicKey=command["from"])
                graph.merge(Subgraph([toWallet, fromWallet]), ('publicKey'))

                ab = Relationship(userCommand, "INCLUDED_IN", currentBlock)
                ac = Relationship(userCommand, "TO", toWallet)
                ad = Relationship(userCommand, "FROM", fromWallet)
                # Commit to Graph
                graph.merge(Subgraph([userCommand], [ab, ac, ad]))
                
            

if __name__ == "__main__":
    main()

