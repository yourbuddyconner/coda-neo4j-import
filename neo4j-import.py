import json
from py2neo import Graph, Node, Relationship, Subgraph

graph = Graph()

def main():

    with open('blocks.json') as json_file:
        data = json.load(json_file)
        for block in data['blocks']["nodes"]:
            # Model Current Block
            currentBlock = Node("Block", stateHash=block["stateHash"])
            graph.merge(Subgraph([currentBlock]), ("stateHash"))

            creatorWallet = Node("Wallet", publicKey=block["creator"])
            ab = Relationship(creatorWallet, "CREATED", currentBlock)
            graph.merge(Subgraph([creatorWallet]), ("publicKey"))
            graph.merge(ab)

            # Model Parent Relationship (If Possible)
            # parentBlock = graph.find_one(label="Block", property_key="stateHash", property_value=block["protocolState"]["previousStateHash"])
            # if not parentBlock:
            # parentBlock =  Node("Block", stateHash=block["protocolState"]["previousStateHash"])
            # parentRelationship = Relationship(currentBlock, "IS_PARENT", parentBlock)
            # graph.merge(Subgraph([parentBlock]), ("statehash"))
            # graph.merge(parentRelationship)

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
                    "amount": int(command["amount"]),
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

            # Model Fee Transfers
            # feeTransfers = block["transactions"]["feeTransfer"]
            # for transfer in feeTransfers:
            #     toWallet = Node("Wallet", publicKey=transfer["recipient"])
            #     fromWallet = Node("Wallet", publicKey=block["creator"])
            #     graph.merge(Subgraph([toWallet, fromWallet]), ('publicKey'))

            #     transferNode = Node("FeeTransfer", amount=int(transfer["fee"]), id=block["stateHash"]+transfer["recipient"])

            #     ab = Relationship(transferNode, "INCLUDED_IN", currentBlock)
            #     ac = Relationship(transferNode, "TO", toWallet)
            #     ad = Relationship(transferNode, "FROM", fromWallet)
            #     graph.merge(Subgraph([transferNode], [ab, ac, ad]))
            
            # Model SNARK Jobs
            snarkJobs = block["snarkJobs"]
            for job in snarkJobs:
                jobNode = Node("SnarkJob", fee=job["fee"], id=block["stateHash"]+job["prover"])
                proverNode = Node("Wallet", publicKey=job["prover"])
                ab = Relationship(jobNode, "CREATED_BY", proverNode)
                ac = Relationship(jobNode, "BOUGHT_BY", creatorWallet)
                ad = Relationship(jobNode, "INCLUDED_IN", currentBlock)
                graph.merge(proverNode, ("publicKey"))
                graph.merge(Subgraph([jobNode], [ab, ac, ad]))

                for work in job["workIds"]:
                    workNode = Node("SnarkWork", workId=work)
                    ab = Relationship(workNode, "INCLUDED_IN", jobNode)
                    graph.merge(Subgraph([workNode], [ab]))

if __name__ == "__main__":
    main()

