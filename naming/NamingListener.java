package naming;

import rmi.RMIException;
import rmi.Skeleton;

import java.net.InetSocketAddress;

public class NamingListener {

    NamingServer namingServer;
    Registration regStub;
    Service servStub;
    Skeleton<Registration> regSkeleton;
    Skeleton<Service> servSkeleton;
    RegistrationListener regListener;

    public NamingListener(NamingServer namingServer) {
        this.namingServer = namingServer;
        initializeStubs();
    }



    public void initializeStubs() {
        String hostname = "127.0.0.1";

        // Create registration stub
        InetSocketAddress regAddr = new InetSocketAddress(hostname,NamingStubs.REGISTRATION_PORT);
        regSkeleton = new Skeleton<Registration>(Registration.class,namingServer, regAddr);
        try {

            regSkeleton.start();
        } catch (RMIException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        regStub = NamingStubs.registration(hostname);

        // Create service stub
        InetSocketAddress servAddr = new InetSocketAddress(hostname,NamingStubs.SERVICE_PORT);
        servSkeleton = new Skeleton<Service>(Service.class, namingServer,servAddr);
        try {
            servSkeleton.start();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        servStub = NamingStubs.service(hostname);

        regListener = new RegistrationListener(regStub);
        regListener.start();
    }

    public void stopListeners() {
        regListener.stopGracefully();
        regSkeleton.stop();
        servSkeleton.stop();
    }
}
