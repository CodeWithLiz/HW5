"""
Where solution code to HW5 should be written.  No other files should
be modified.
"""

from concurrent.futures import wait
import socket
import io
import time
import typing
import struct
import homework5
import homework5.logging


# change the inside of this function to implement your solution to the sending logic
# we should You should
#do so through a combination of setting timeouts on socket reads 
# and using the select module to wait for the socket to be ready for reading or writing.
# we socket.timeout(float)`) and developing a system through which each side can
# acknowledge if / when they receive a packet.
# no more than 2 packets should be in flight at any given time, and the sender should wait for an acknowledgment from the receiver before sending more data.
# else case here if there is 3 packet  while there is 2 traveling in the network , just drop the third packet 
# to test are soultion we can tester.py
def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """

    # Naive implementation where we chunk the data to be sent into
    # packets as large as the network will allow, and then send them
    # over the network, pausing half a second between sends to let the
    # network "rest" :)

    #read file data 
    # sends packets
    #waits for ack 
    # handle timeouts 


    logger = homework5.logging.get_logger("hw5-sender")
    chunk_size = homework5.MAX_PACKET - 4 
    pause = .1
    offsets = range(0, len(data), chunk_size)
    alpha = 0.125  # Weight for the new sample RTT
    estimated_rtt = 1.0  # Initial estimated RTT
    dev_rtt = 0.0  # Deviation of RTT for the current packet
    timeout = estimated_rtt + 4 * dev_rtt  # Initial timeout based on estimated RTT and deviation
    
    # splits the files into chunks 

   

 
  
    
    for index, chunk in enumerate([data[i:i + chunk_size] for i in offsets]):
          #converts the. index to a 4-byte sequence number for comparison
        seqence_number = struct.pack('!I', index)  # Pack the sequence number
        pack = seqence_number + chunk  # Create the packet with sequence number and data
        #this is a issue because we are not waiting for ack before sending the next packet
        send_time = time.monotonic()
        sock.send(pack)
        ack = b'' 

        reTransmited = False # checks if the same packet is being sent again 

             # caluate timeoute 
        timeout = estimated_rtt + 4 * (dev_rtt)  # Calculate timeout based on estimated RTT
        deadline = time.monotonic() + timeout  # Calculate the deadline for receiving ACK
       
        while ack != seqence_number:  # Wait for the correct ACK
            reamaining_time = deadline - time.monotonic()  # Calculate remaining time for timeout
        

            try:
           
                if reamaining_time <= 0:
                    print("deadline expired")
                    reTransmited = True  # Mark that the packet was retransmitted
                    deadline = time.monotonic() + timeout  # Reset the deadline for receiving ACK
                    sock.send(pack)  # Resend the packet if the deadline has expired
                    continue  # Continue to the next iteration to wait for ACK again
                else:
                    sock.settimeout(reamaining_time)  # Set the socket timeout to the remaining time
             

                # ack is 4 bytes long, we will wait for the ack to be received
                ack = sock.recv(4)  
                ack_time = time.monotonic()

              
               
                #if ack is different from the sequence number we sent, we will resend the packet
                if ack != seqence_number:
                    logger.warning("old ack")
                    continue

                elif ack == seqence_number:
                
                    # this checks if the packet was retransmitted or not, if it was not retransmitted we will log the rtt
                    if not reTransmited:
                        # calculate sample_rtt and update estimated_rtt and dev_rtt
                        sample_rtt = ack_time - send_time
                        # update dev_rtt using the old estimated_rtt
                        dev_rtt = 0.25 * abs(sample_rtt - estimated_rtt) + 0.75 * dev_rtt  # Calculate deviation of RTT
                        estimated_rtt = (1 - alpha) * estimated_rtt + alpha * sample_rtt
                        timeout = estimated_rtt + 4 * dev_rtt  # Update timeout based on new estimated RTT and deviation
                        logger.info("Received ACK for packet %d, RTT: %.4f seconds", index, sample_rtt)
                    break  # Exit the loop if the correct ACK is received            
                    

                    
              
            
                

                    # this handles not receiving ack and resending them 
            except socket.timeout:
                logger.warning("Timeout waiting for ACK, resending packet")
                reTransmited = True  # Mark that the packet was retransmitted
                sock.send(pack)  # Resend the packet on timeout
                deadline = time.monotonic() + timeout  # Reset the deadline for receiving ACK
    # FIN HANDSHAKE: Signal that all data packets have been sent

    # Create the special FIN packet
    fin_number = 0xFFFFFFFF
    fin_packet = struct.pack('!I', fin_number)

# Send FIN
    sock.send(fin_packet)

    # Set the deadline for FIN-ACK
    fin_deadline = time.monotonic() + timeout

    # Wait for FIN-ACK
    fin_ack = b''

    while fin_ack != fin_packet:

        # Calculate the remaining waiting time
        fin_remaining_time = fin_deadline - time.monotonic()

        # If the deadline expired, retransmit FIN
        if fin_remaining_time <= 0:
            sock.send(fin_packet)
            fin_deadline = time.monotonic() + timeout
            continue

        try:
            # Wait only for the remaining time
            sock.settimeout(fin_remaining_time)

            # Receive FIN-ACK
            fin_ack = sock.recv(4)

            # Correct FIN-ACK received
            if fin_ack == fin_packet:
                break

        except socket.timeout:
            logger.warning("Timeout waiting for FIN-ACK")

            # Retransmit FIN
            sock.send(fin_packet)

            # Reset the deadline
            fin_deadline = time.monotonic() + timeout
            #  logger.info("Pausing for %f seconds", round(pause, 2))
            #  time.sleep(pause)







# Inside onf recev function is too recieves packets 
# RECONGIZES duplicates packets , ack data and recreates the data 
def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """

    logger = homework5.logging.get_logger("hw5-receiver")
    # Naive solution, where we continually read data off the socket
    # until we don't receive any more data, and then return.


    # the issue here is that we are not handling duplicates and we are not sending ack back to the sender
    num_bytes = 0
    expected_sequence_number = 0  # Initialize the expected sequence number


    fin_number = 0xFFFFFFFF
    fin_ack = struct.pack('!I', fin_number)

    fin_received = False

    fin_wait_time = 5.0  # Time to wait after receiving FIN before shutting down
    fin_deadline = None
    while True:
        
    # STEP 1: If FIN was already received, configure
    # a temporary shutdown waiting period.
        if fin_received:
            remaining_time = fin_deadline - time.monotonic()
            if remaining_time <= 0:
                logger.info("FIN wait time expired, shutting down receiver")
                logger.warning("Receiver exiting FIN shutdown; fin_received=%s",fin_received
)
                break
            else:
                sock.settimeout(remaining_time)

    # STEP 2: Receive a packet, handling timeout.
        try:
            data = sock.recv(homework5.MAX_PACKET)

        except socket.timeout:
            if fin_received:
                break
            else:
                continue

    # Keep your existing short-packet check below
        if len(data) < 4:
            logger.warning("Received packet is too short to contain a sequence number")
            continue  # Skip processing this packet if it's too short

        sequence_number = data[:4]  # Extract the sequence number from the received packet
        received_sequence_number = struct.unpack('!I', sequence_number)[0]  # Unpack the sequence number


        # STEP 3: Check for FIN first
        if received_sequence_number == fin_number:
            if not fin_received:
                fin_deadline = time.monotonic() + fin_wait_time
            logger.info("FIN packet received")
            sock.send(fin_ack)
            fin_received = True
            continue



        if received_sequence_number == expected_sequence_number:
            # write the payload to the destination if the sequence number matches
            dest.write(data[4:])  # Write the payload to the destination
            num_bytes += len(data) - 4  # Update the number of bytes written (excluding the sequence number)
            expected_sequence_number += 1  # Increment the expected sequence number

            ack = struct.pack('!I', received_sequence_number)  # Pack the sequence number for ACK
            sock.send(ack)  # Send ACK back to the sender
        else:
            #handle are duplicates 
            if received_sequence_number < expected_sequence_number:
                logger.warning("Duplicate packet received: %d, expected: %d", received_sequence_number, expected_sequence_number)

                # create ack 
                ack = struct.pack('!I', received_sequence_number)  # Pack the sequence number for ACK
                sock.send(ack)  # Send ACK back to the sender for the duplicate packe

               

            elif received_sequence_number > expected_sequence_number:
                logger.warning("Out of order packet received: %d, expected: %d", received_sequence_number, expected_sequence_number)

       # if not data:
          #  break
        logger.info("Received %d bytes", len(data))
       # dest.write(data)
      #  num_bytes += len(data)
        dest.flush()
    return num_bytes
