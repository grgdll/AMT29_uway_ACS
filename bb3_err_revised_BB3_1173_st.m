%this function propagates uncertainties through the computation of bbp from raw bb3-1173 counts
function [bbp err rel_err bdgt_X bdgt_SF bdgt_C bdgt_Bw bdgt_DC bdgt_WE Btot] = bb3_err_revised_BB3_1173_st(C, sC, X, Salin, T)



% %parameters
    DC = [50 42 46];  %dark counts from WETLabs
    DC_err = [1.2 1.2 1.2];  % uncertainties in dark counts

    SF = [11.0 7.127 2.983 ]*1e-6;  %scaling factors from WETLabs
    
    
% %use this for computing scaling factor of bbrtd167 (it was computed just
% %before the collection of bbrtd167 data simultaneously with bb349 data
%     SF = [6.4316e-006  4.496e-006 2.3227e-006];  %scaling factors from first calibration during AMT19 cruise

    
    wv = [470 532 700];
    
% wall effect 
          
     WE =     [ 3.7029  3.1437  4.2641].*1e-04;%<< == derived from the data collected in the lab with Toby on 04/08/10 (ie Apr. 8th, 2010)
     WE_err = [ 0.8327  0.5218  0.2703].*1e-04;  
     
     
     
        
%
%    %compute Bw using the coefficients provided by Zhang et al. 2009
%    %    using this model we decrease the error of betasw from 15% to sqrt( (2%_accuracy_of_bw)^2  +(1%_accuracy of salinity correction)^2  )
%    %    (the accuracy is the one declared by Zhang and Hu 2009 + the one on the salinity correction (average bias 1%))
%        [theta,betasw,bsw] = betasw_ZHH2009(wv(1:3),Salin,T); %@470nm
%        %a117 = find(theta == 117);
%%        Bw(1:3) = betasw(theta == 117,1:3);
%        Bw(1:3) = betasw(theta == 124,1:3) * 1.0121;  % NOTE: trapz(theta, W_Sullivan.*betasw)./trapz(theta, W_Sullivan) = 1.0121 * betasw(124) INDEPENDENT OF WAVELENGTH  this is the weighting by the angular function W
 
        betasw124 = betasw124_ZHH2009(wv(1:3),Salin,T); 
%        betasw117 = betasw117_ZHH2009(wv(1:3),Salin,T); 
        Bw = real(betasw124) * 1.0121;  % this is the weighting by the angular function W NOTE: trapz(theta, W_Sullivan.*betasw)./trapz(theta, W_Sullivan) = 1.0121 * betasw(124) INDEPENDENT OF WAVELENGTH  




                
    CnoDC = C-ones(size(C))*diag(DC);  %correction for dark current
    Btot = CnoDC*diag(SF);    
    Bp = Btot - Bw;  %remove Bw from Btot
    



    %this is the traditional way
        bbp = Bp.*2*pi*X  -  ones(size(Btot))*diag(WE);    %result of the integration of Bp(117degs) over the backward direction  minus the bbp due to the wall effect
    
%     %this one is using the bbw model provided by Jonasz and Fournier
%         bbw = JF(wv,Salin,T);
%         bbw = bbw(ones(length(sC),1),:);
%         bbp = 2*pi*X*Btot-bbw;



    
%derivatives
    dBBPdX = bbp./X;
        
    dBBPdSF = 2*pi*X.*CnoDC;
        
    dBBPdC = [2*pi*X.*diag(SF)*ones(size(Bp))']';

    dBBPdBw = -2*pi*X.*ones(size(Bp));
    
    dBBPdDC = [-2*pi*X.*diag(SF)*ones(size(Bp))']';

    
    

%uncertainties
    %--note after talking to Mike T. it seems that this uncertainty could be around 0.10  (he said that X is just a little bit smaller than 1)
    if(X == 0.9)
        sX = X*0.011111.*ones(size(Bp));  %as suggested by Sullivan et al 2005
    else
        %sX = X*0.04.*ones(size(Bp)); %as in Boss and Pegau 2001
        sX = X*0.0292*ones(size(Bp)); %as in Sullivan and Twardowski 2009
    end
    
%    errSF = [1.6958e-07   2.1240e-08   1.5061e-07];   %these are the stdev/sqrt(2) of my two determinations carried out during AMT19   /users/modellers/gdal/Desktop/AMT19/Processed/Wap_extracted/Calibr_2/res_calibr_final.txt res_cal1.txt  res_cal2.txt 
    %errSF = [0.6 3.9 14.1].*SF/100;   %these are the percent deviations from the average of my two determinations carried out during AMT19 and the previous WetLabs calibration
%    errSF = 0.05*SF;   % these estimates are based on Sullivan et al. 2012 Light scattering review vol. 7
    errSF = [0.082911   0.084128   0.084440].*SF;   % these estimates are based on the 8% from Sullivan et al. 2012 Light scattering review vol. 7
                                                    % and on the ste of the mean of the cruise cals
                                                    % sqrt([0.08^2 + [0.021778   0.026028   0.027020].^2]) = [0.082911   0.084128   0.084440]
    
    
    sSF = [diag(errSF)*ones(size(Bp))']';
    
    sC = sC;  %given as input
    
    sBw = [0.0224*Bw];%<<< this is to allow for vector Salinity and Temperature (because betaw_zhanghu09 is within the 2% error of Morel's measurements and the salinity correction is within 1% (in average))
%    sBw = [0.0224*diag(Bw)*ones(size(Bp))']';%<<< == == because betaw_zhanghu09 is within the 2% error of Morel's measurements and the salinity correction is within 1% (in average)
    %sBw = [0.078.*diag(Bw)*ones(size(Bp))']';  %the 7.8% uncertainty is from the improved Bsw estimates  = sqrt(6%^2+5%^2), where 6% is the relative uncertainty in Bw reported by buiteveld et al, and 5% is the relative uncertainy estimated by Twardowski for the salinity correction
    %sBw = 0.1221.*ones(size(Bp))*diag(Bw);  %the sqrt(7%^2+10%^2) uncertainty is from the improved Bw estimates + the salinity uncertainty in b_sw taken from Shifrin

    sDC = ones(size(Bp))*diag(DC_err);    %std of measurements taken during AMT22
    
    sWE = [diag(WE_err)*ones(size(Bp))']';  %uncertainty due to the wall effect correction
    
    
%error computation
    err = sqrt( (dBBPdX .*sX ).^2     + (dBBPdSF.*sSF).^2     + (dBBPdC .*sC ).^2     + (dBBPdBw.*sBw).^2    + (dBBPdDC.*sDC).^2   +sWE.^2);
    %err = sqrt(  (dBBPdDC.*sDC).^2   +sWE.^2);  %this is only to understand the effect of biases on the validation
    rel_err = err./bbp;

%     %these values are relative SQUARED contributions of each uncertainty factor to the total SQUARED error
%     bdgt_X = (dBBPdX .*sX ./err).^2;
%     bdgt_SF = (dBBPdSF.*sSF./err).^2;
%     bdgt_C = (dBBPdC .*sC ./err).^2;
%     bdgt_Bw = (dBBPdBw.*sBw ./err).^2;
%     bdgt_DC = (dBBPdDC.*sDC ./err).^2;
%     bdgt_WE = (sWE ./err).^2;
        
    %these values are absolute contributions of each uncertainty factor to the total error
    bdgt_X = abs(dBBPdX .*sX );
    bdgt_SF = abs(dBBPdSF.*sSF);
    bdgt_C = abs(dBBPdC .*sC );
    bdgt_Bw = abs(dBBPdBw.*sBw );
    bdgt_DC = abs(dBBPdDC.*sDC );
    bdgt_WE = abs(sWE );
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%      check with MC                        %%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%NOTE: for this calculation it is necessary to use as input to bb3_err only one vlaue of C and sC for each wavelength
%      also set sC = 0
    
%     rnd_SF = randn(1000,3)*diag(sSF) +  ones(1000,3)*diag(SF)  ;
%     rnd_Bw = randn(1000,3)*diag(sBw) +  ones(1000,3)*diag(Bw)  ;
%     rnd_X = randn(1000,3) *diag(0.04*1.1)  +  1.1 ;
%     
%     % figure
%     % hist(rnd_SF(:,1), 100)
%     % figure
%     % hist(rnd_Bw(:,1), 100)
%     % figure
%     % hist(rnd_X(:,1), 100)
%     
%     % [SF; median(rnd_SF)]
%     % [Bw; median(rnd_Bw)]
%     % [[X X X]; median(rnd_X)]
%     
%     
%     
%      bbp_MC = 2*pi.*rnd_X.*( rnd_SF*diag(CnoDC) - rnd_Bw );
%     %  figure
%     %  hist(bbp_MC(:,1), 100)
%     
%      [bbp; median(bbp_MC)]
%      [err; std(bbp_MC)]
%      
     

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

endfunction
